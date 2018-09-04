#include "raft_fsm.h"

#include <random>
#include <sstream>

#include "logger.h"
#include "raft_exception.h"
#include "ready.h"
#include "replica.h"
#include "storage/storage_disk.h"
#include "storage/storage_memory.h"

namespace sharkstore {
namespace raft {
namespace impl {

RaftFsm::RaftFsm(const RaftServerOptions& sops, const RaftOptions& ops)
    : sops_(sops),
      rops_(ops),
      node_id_(sops.node_id),
      id_(ops.id),
      sm_(ops.statemachine) {
    auto s = start();
    if (!s.ok()) {
        throw RaftException(s);
    }
}

int RaftFsm::numOfPendingConf(const std::vector<EntryPtr>& ents) {
    int n = 0;
    for (const auto& ent : ents) {
        if (ent->type() == pb::ENTRY_CONF_CHANGE) ++n;
    }
    return n;
}

// 从Message里提取日志，使用swap，原Message里的日志内容会被清空
void RaftFsm::takeEntries(MessagePtr& msg, std::vector<EntryPtr>& ents) {
    int size = msg->entries_size();
    if (size > 0) {
        ents.reserve(size);
        for (int i = 0; i < size; ++i) {
            ents.emplace_back(std::make_shared<pb::Entry>());
            ents.back()->Swap(msg->mutable_entries(i));
        }
    }
}

void RaftFsm::putEntries(MessagePtr& msg, const std::vector<EntryPtr>& ents) {
    for (const auto& e : ents) {
        auto p = msg->add_entries();
        p->CopyFrom(*e);  // must copy
    }
}

Status RaftFsm::loadState(const pb::HardState& state) {
    // 全空
    if (state.commit() == 0 && state.term() == 0 && state.vote() == 0) {
        return Status::OK();
    }

    if (state.commit() < raft_log_->committed() ||
        state.commit() > raft_log_->lastIndex()) {
        std::stringstream ss;
        ss << "loadState: state.commit " << state.commit() << " is out of range ["
           << raft_log_->committed() << ", " << raft_log_->lastIndex() << "]";
        return Status(Status::kCorruption, ss.str(), "");
    }

    term_ = state.term();
    vote_for_ = state.vote();
    raft_log_->commitTo(state.commit());

    return Status::OK();
}

Status RaftFsm::recoverCommit() {
    Status s;
    while (raft_log_->applied() < raft_log_->committed()) {
        std::vector<EntryPtr> committed_entries;
        raft_log_->nextEntries(64 * 1024 * 1024, &committed_entries);
        for (const auto& entry : committed_entries) {
            raft_log_->appliedTo(entry->index());
            if (entry->type() == pb::ENTRY_CONF_CHANGE) {
                s = applyConfChange(entry);
                if (!s.ok()) {
                    return s;
                }
            }
            s = smApply(entry);
            if (!s.ok()) {
                return s;
            }
        }
    }
    return s;
}

Status RaftFsm::start() {
    // 初始化随机函数(选举超时)
    unsigned seed = static_cast<unsigned>(
        std::chrono::system_clock::now().time_since_epoch().count() * node_id_);
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<unsigned> distribution(sops_.election_tick,
                                                         sops_.election_tick * 2);
    random_func_ = std::bind(distribution, engine);

    // 初始化raft日志
    if (rops_.use_memory_storage) {
        storage_ =
            std::shared_ptr<storage::Storage>(new storage::MemoryStorage(id_, 40960));
        LOG_WARN("raft[%llu] use raft logger memory storage!", id_);
    } else {
        storage::DiskStorage::Options ops;
        ops.log_file_size = rops_.log_file_size;
        ops.max_log_files = rops_.max_log_files;
        ops.allow_corrupt_startup = rops_.allow_log_corrupt;
        ops.initial_first_index = rops_.initial_first_index;
        storage_ = std::shared_ptr<storage::Storage>(
            new storage::DiskStorage(id_, rops_.storage_path, ops));
    }
    auto s = storage_->Open();
    if (!s.ok()) {
        LOG_ERROR("raft[%llu] open raft storage failed: %s", id_, s.ToString().c_str());
        return Status(Status::kCorruption, "open raft logger", s.ToString());
    } else {
        raft_log_ = std::unique_ptr<RaftLog>(new RaftLog(id_, storage_));
    }

    // 加载 hardstate
    pb::HardState hs;
    s = storage_->InitialState(&hs);
    if (!s.ok()) {
        return s;
    }
    s = loadState(hs);
    if (!s.ok()) {
        return s;
    }

    // 初始化成员
    bool local_found = false;
    for (const auto& p : rops_.peers) {
        if (p.node_id == node_id_) {
            local_found = true;
            is_learner_ = (p.type == PeerType::kLearner);
        }
        if (p.type == PeerType::kNormal) {
            replicas_.emplace(p.node_id, newReplica(p, false));
        } else {
            learners_.emplace(p.node_id, newReplica(p, false));
        }
    }
    if (!local_found) {
        return Status(Status::kInvalidArgument, std::string("could not find local node(") +
                  std::to_string(node_id_) + ") in peers", PeersToString(rops_.peers));
    }

    LOG_INFO("newRaft[%llu]%s commit: %llu, applied: %llu, lastindex: %llu, "
             "peers: %s",
             id_, (is_learner_ ? " [learner]" : ""), raft_log_->committed(),
             rops_.applied, raft_log_->lastIndex(), PeersToString(rops_.peers).c_str());

    if (rops_.applied > 0) {
        uint64_t lasti = raft_log_->lastIndex();
        if (lasti < rops_.applied) {
            raft_log_->set_committed(lasti);
            rops_.applied = lasti;
        } else if (raft_log_->committed() < rops_.applied) {
            raft_log_->set_committed(rops_.applied);
        }
        raft_log_->appliedTo(rops_.applied);
    }

    s = recoverCommit();
    if (!s.ok()) {
        return s;
    }

    // 初始状态
    do {
        // 没有指定leader
        if (rops_.leader == 0) {
            becomeFollower(term_, 0);
            break;
        }
        // 指定了leader，但是term非法或者小于当前term
        if (rops_.term == 0 || rops_.term < term_) {
            becomeFollower(term_, 0);
            break;
        }
        // 指定的leader不是本节点，转为指定term的follower
        if (rops_.leader != node_id_) {
            becomeFollower(rops_.term, rops_.leader);
            break;
        }
        // 本节点是个learner，不能成为leader
        if (is_learner_) {
            becomeFollower(term_, 0);
            break;
        }
        // 指定的leader是本节点
        if (term_ != rops_.term) {
            term_ = rops_.term;
            vote_for_ = 0;
        }
        state_ = FsmState::kLeader;  // avoid invalid transition check
        becomeLeader();
    } while (false);

    return Status::OK();
}

bool RaftFsm::Validate(MessagePtr& msg) const {
    // 过滤未知来源的response
    // 不过滤request是因为副本有可能落后，集群成员不是最新的
    return IsLocalMsg(msg) || hasReplica(msg->from()) || !IsResponseMsg(msg);
}

bool RaftFsm::stepIngoreTerm(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_TICK:
            tick_func_();
            return true;

        case pb::LOCAL_MSG_HUP: {
            if (state_ != FsmState::kLeader && electable()) {
                std::vector<EntryPtr> ents;
                Status s = raft_log_->slice(raft_log_->applied() + 1,
                                            raft_log_->committed() + 1, kNoLimit, &ents);
                if (!s.ok()) {
                    throw RaftException(
                        std::string("[Step] unexpected error when getting "
                                    "unapplied entries:") +
                        s.ToString());
                }

                if (numOfPendingConf(ents) != 0) {
                    LOG_INFO("raft[%llu] pending conf exist. campaign forbidden", id_);
                } else {
                    campaign(sops_.enable_pre_vote);
                }
            }
            return true;
        }

        case pb::LOCAL_MSG_PROP:
            step_func_(msg);
            return true;

        case pb::HEARTBEAT_REQUEST:
            // 只接受来自leader的心跳
            if (msg->from() == leader_ && msg->from() != node_id_) {
                step_func_(msg);
            }
            return true;

        case pb::HEARTBEAT_RESPONSE:
            // 只有leader才需要处理心跳回应
            if (state_ == FsmState::kLeader && msg->from() != node_id_) {
                step_func_(msg);
            }
            return true;

        default:
            return false;
    }
}

void RaftFsm::stepLowTerm(MessagePtr& msg) {
    if (msg->type() == pb::PRE_VOTE_REQUEST) {
        LOG_INFO("raft[%lu] [logterm: %lu, index: %lu, vote: %lu] "
                 "rejected low term %s from %lu [logterm: %lu, "
                 "index: %lu] at term %lu.",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 MessageType_Name(msg->type()).c_str(), msg->from(), msg->log_term(),
                 msg->log_index(), term_);

        // 本节点有可能未启用PreVote，日志不新但是term很大
        // 如果丢弃该消息，就导致来源节点term升不上去，可能永远无法选举成功,
        // 我们给它回应，可以让来源节点的term升上去
        MessagePtr resp(new pb::Message);
        resp->set_type(pb::PRE_VOTE_RESPONSE);
        resp->set_to(msg->from());
        resp->set_reject(true);
        send(resp);
    } else if (sops_.enable_pre_vote && msg->type() == pb::APPEND_ENTRIES_REQUEST) {
        // 收到了来自低term leader(APPEND只会由leader发出)的消息，原因可能是
        // 1) 来源leader被分区出去
        // 2) 本节点选举增加了term，因为启用了PreVote，不会随意提升term
        // 两种情况让来源leader退位是安全的
        // 如果它是一个被网络延迟的消息，我们的回复里log_term和commit都是0
        // 也不会干扰正常leader的复制，让leader错误以为前面发起的某次复制是成功的

        LOG_INFO("raft[%llu] ignore a [%s] message from low term leader [%llu "
                 "term: %llu] at term %llu",
                 id_, MessageType_Name(msg->type()).c_str(), msg->from(), msg->term(),
                 term_);

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
        resp->set_to(msg->from());
        send(resp);
    } else {
        LOG_INFO("raft[%llu] ignore a [%s] message with lower term "
                 "from [%llu term: %llu] at term %llu",
                 id_, MessageType_Name(msg->type()).c_str(), msg->from(), msg->term(),
                 term_);
    }
}

void RaftFsm::stepVote(MessagePtr& msg, bool pre_vote) {
    // learner不参与投票
    if (is_learner_) {
        LOG_INFO("raft[%lu] [logterm:%lu, index:%lu, vote:%lu] ignore %s from "
                 "%lu[logterm:%lu, index:%lu] at term %lu: learner can not "
                 "vote.",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 (pre_vote ? "pre-vote" : "vote"), msg->from(), msg->log_term(),
                 msg->log_term(), term_);
        return;
    }

    // 如果非PreVote，此时msg term已经和当前term一致

    MessagePtr resp(new pb::Message);
    resp->set_type(pre_vote ? pb::PRE_VOTE_RESPONSE : pb::VOTE_RESPONSE);
    resp->set_to(msg->from());

    // 可以投票条件：
    // 1)  当前term已经给它投过票
    // 2)  当前term没给其他人投过票，且当前term没有leader
    // 3)  如果是prevote则需要msg的term(加过1的)大于我们的term
    bool can_vote = (vote_for_ == msg->from()) || (vote_for_ == 0 && leader_ == 0) ||
                    (msg->type() == pb::PRE_VOTE_REQUEST && msg->term() > term_);

    // 比较日志新旧
    if (can_vote && raft_log_->isUpdateToDate(msg->log_index(), msg->log_term())) {
        LOG_INFO("raft[%llu] [logterm:%llu, index:%llu, vote:%llu] %s for "
                 "%llu[logterm:%llu, index:%llu] at term %llu",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 (pre_vote ? "pre-vote" : "vote"), msg->from(), msg->log_term(),
                 msg->log_index(), term_);

        resp->set_term(msg->term());
        resp->set_reject(false);
        send(resp);

        if (msg->type() == pb::VOTE_REQUEST) {
            vote_for_ = msg->from();
            election_elapsed_ = 0;
        }
    } else {
        LOG_INFO("raft[%llu] [logterm:%llu, index:%llu, vote:%llu] %s for "
                 "%llu[logterm:%llu, index:%llu] at term %llu",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 (pre_vote ? "reject vote(pre)" : "reject vote"), msg->from(),
                 msg->log_term(), msg->log_index(), term_);

        resp->set_term(term_);
        resp->set_reject(true);
        send(resp);
    }
}

void RaftFsm::Step(MessagePtr& msg) {
    // 处理不需要关心term的消息类型
    if (stepIngoreTerm(msg)) {
        return;
    }

    // 处理低term msg
    if (msg->term() < term_) {
        stepLowTerm(msg);
        return;
    }

    // 高term，先变为follower再处理msg
    if (msg->term() > term_) {
        if (msg->type() == pb::PRE_VOTE_REQUEST ||
            (msg->type() == pb::PRE_VOTE_RESPONSE && !msg->reject())) {
            // 1) prevote请求的term是来源自身的term+1，不是其真正的term
            // 2) 如果是prevote回应是成功，回应的term是请求时的term（即+1后的）
            //    我们是收到大多数prevote成功后才会增加term，不应该此时加
        } else {
            LOG_INFO("raft[%llu] received a [%s] message with higher term "
                     "from [%llu term: %llu] at term %llu, become follower",
                     id_, MessageType_Name(msg->type()).c_str(), msg->from(), msg->term(),
                     term_);

            if (msg->type() == pb::APPEND_ENTRIES_REQUEST ||
                msg->type() == pb::SNAPSHOT_REQUEST) {
                becomeFollower(msg->term(), msg->from());
            } else {
                becomeFollower(msg->term(), 0);
            }
        }
    }

    if (msg->type() == pb::PRE_VOTE_REQUEST) {
        stepVote(msg, true);
    } else if (msg->type() == pb::VOTE_REQUEST) {
        stepVote(msg, false);
    } else {
        step_func_(msg);
    }
}

void RaftFsm::GetReady(Ready* rd) {
    rd->committed_entries.clear();
    raft_log_->nextEntries(kNoLimit, &(rd->committed_entries));

    rd->msgs = std::move(sending_msgs_);

    if (sending_snap_ && !sending_snap_->IsDispatched()) {
        rd->send_snap = sending_snap_;
        sending_snap_->MarkAsDispatched();
    } else {
        rd->send_snap = nullptr;
    }

    if (applying_snap_ && !applying_snap_->IsDispatched()) {
        rd->apply_snap = applying_snap_;
        applying_snap_->MarkAsDispatched();
    } else {
        rd->apply_snap = nullptr;
    }
}

std::tuple<uint64_t, uint64_t> RaftFsm::GetLeaderTerm() const {
    return std::make_tuple(leader_, term_);
}

pb::HardState RaftFsm::GetHardState() const {
    pb::HardState hs;
    hs.set_term(term_);
    hs.set_vote(vote_for_);
    hs.set_commit(raft_log_->committed());
    return hs;
}

Status RaftFsm::Persist(bool persist_hardstate) {
    // 持久化日志
    std::vector<EntryPtr> ents;
    raft_log_->unstableEntries(&ents);
    if (!ents.empty()) {
        auto s = storage_->StoreEntries(ents);
        if (!s.ok()) {
            return Status(Status::kIOError, "store entries", s.ToString());
        } else {
            raft_log_->stableTo(ents.back()->index(), ents.back()->term());
        }
    }
    // 持久化HardState
    if (persist_hardstate) {
        auto hs = GetHardState();
        auto s = storage_->StoreHardState(hs);
        if (!s.ok()) {
            return Status(Status::kIOError, "store hardstate", s.ToString());
        }
    }
    return Status::OK();
}

std::vector<Peer> RaftFsm::GetPeers() const {
    std::vector<Peer> peers;
    traverseReplicas([&](uint64_t id, const Replica& pr) { peers.push_back(pr.peer()); });
    return peers;
}

RaftStatus RaftFsm::GetStatus() const {
    RaftStatus s;
    s.node_id = sops_.node_id;
    s.leader = leader_;
    s.term = term_;
    s.index = raft_log_->lastIndex();
    s.commit = raft_log_->committed();
    s.applied = raft_log_->applied();
    s.state = FsmStateName(state_);
    if (state_ == FsmState::kLeader) {
        traverseReplicas([&](uint64_t node, const Replica& pr) {
            ReplicaStatus rs;
            rs.peer = pr.peer();
            rs.match = pr.match();
            rs.commit = pr.committed();
            rs.next = pr.next();
            // leader self is always active
            if (node != node_id_) {
                auto inactive_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                        pr.inactive_ticks() * sops_.tick_interval) .count();
                rs.inactive_seconds = static_cast<int>(inactive_seconds);
            }
            rs.state = ReplicateStateName(pr.state());
            rs.snapshotting = pr.state() == ReplicaState::kSnapshot;
            s.replicas.emplace(node, rs);
        });
    }
    return s;
}

Status RaftFsm::TruncateLog(uint64_t index) {
    if (applying_snap_) return Status::OK();
    return storage_->Truncate(index);
}

Status RaftFsm::DestroyLog(bool backup) { return storage_->Destroy(backup); }

Status RaftFsm::smApply(const EntryPtr& entry) {
    Status s;
    switch (entry->type()) {
        case pb::ENTRY_NORMAL:
            if (!entry->data().empty()) {
                return sm_->Apply(entry->data(), entry->index());
            }
            break;

        case pb::ENTRY_CONF_CHANGE: {
            ConfChange cc;
            auto s = DecodeConfChange(entry->data(), &cc);
            if (!s.ok()) return s;
            s = sm_->ApplyMemberChange(cc, entry->index());
            if (!s.ok()) {
                return s;
            }
            break;
        }
        default:
            return Status(Status::kInvalidArgument, "apply unknown raft entry type",
                          std::to_string(static_cast<int>(entry->type())));
    }
    return s;
}

Status RaftFsm::applyConfChange(const EntryPtr& e) {
    assert(e->type() == pb::ENTRY_CONF_CHANGE);

    LOG_INFO("raft[%llu] apply confchange at index %lu, term %lu", id_, e->index(),
             term_);

    pending_conf_ = false;

    ConfChange cc;
    auto s = DecodeConfChange(e->data(), &cc);
    if (!s.ok()) return s;

    if (cc.peer.node_id == 0) {  // invalid peer id
        return Status(Status::kInvalidArgument, "apply confchange invalid node id", "0");
    }

    switch (cc.type) {
        case ConfChangeType::kAdd:
            addPeer(cc.peer);
            break;
        case ConfChangeType::kRemove:
            removePeer(cc.peer);
            break;
        case ConfChangeType::kPromote:
            promotePeer(cc.peer);
            break;
        default:
            return Status(Status::kInvalidArgument, "apply confchange invalid cc type",
                          std::to_string(static_cast<int>(cc.type)));
    }

    return Status::OK();
}

std::unique_ptr<Replica> RaftFsm::newReplica(const Peer& peer, bool is_leader) const {
    if (is_leader) {
        auto r = std::unique_ptr<Replica>(new Replica(peer, sops_.max_inflight_msgs));
        auto lasti = raft_log_->lastIndex();
        r->set_next(lasti + 1);
        if (peer.node_id == node_id_) {
            r->set_match(lasti);
            r->set_committed(raft_log_->committed());
        }
        return r;
    } else {
        // 如果当前节点非leader，则不需要关心副本复制进度等状态
        return std::unique_ptr<Replica>(new Replica(peer));
    }
}

void RaftFsm::traverseReplicas(const std::function<void(uint64_t, Replica&)>& f) const {
    for (const auto& pr : replicas_) {
        f(pr.first, *(pr.second));
    }
    for (const auto& pr : learners_) {
        f(pr.first, *(pr.second));
    }
}

bool RaftFsm::hasReplica(uint64_t node) const {
    return replicas_.find(node) != replicas_.cend() ||
           learners_.find(node) != learners_.cend();
}

Replica* RaftFsm::getReplica(uint64_t node) const {
    auto it = replicas_.find(node);
    if (it != replicas_.cend()) {
        return it->second.get();
    }
    it = learners_.find(node);
    if (it != learners_.cend()) {
        return it->second.get();
    }
    return nullptr;
}

void RaftFsm::addPeer(const Peer& peer) {
    LOG_INFO("raft[%llu] add peer: %s at term %lu", id_, peer.ToString().c_str(), term_);

    auto old = getReplica(peer.node_id);
    if (old != nullptr) {
        LOG_WARN("raft[%llu] add peer already exist: %s", id_,
                 old->peer().ToString().c_str());
        return;
    }

    // make replica
    auto replica = newReplica(peer, state_ == FsmState::kLeader);
    if (peer.type == PeerType::kLearner) {
        learners_.emplace(peer.node_id, std::move(replica));
    } else {
        replicas_.emplace(peer.node_id, std::move(replica));
    }
}

void RaftFsm::removePeer(const Peer& peer) {
    LOG_INFO("raft[%llu] remove peer: %s at term %lu", id_, peer.ToString().c_str(),
             term_);

    auto replica = getReplica(peer.node_id);
    if (replica == nullptr) {
        LOG_WARN("raft[%llu] remove peer doesn't exist: %s, ignore", id_,
                 peer.ToString().c_str());
        return;
    }

    // peer id不一致，不删除，防止旧日志重放时误删除
    if (replica->peer().peer_id != peer.peer_id) {
        LOG_INFO("raft[%llu] ignore remove peer(inconsistent peer id), old: %s, to "
                 "remove: %s",
                 id_, replica->peer().ToString().c_str(), peer.ToString().c_str());
        return;
    }

    // 取消可能正在进行的快照发送
    if (state_ == FsmState::kLeader && sending_snap_ &&
        sending_snap_->GetContext().to == peer.node_id) {
        sending_snap_->Cancel();
        sending_snap_.reset();
    }

    learners_.erase(peer.node_id);
    replicas_.erase(peer.node_id);
    if (peer.node_id == node_id_) {
        // 删除本节点，退位为follower
        becomeFollower(term_, 0);
    } else if (state_ == FsmState::kLeader &&
               (!replicas_.empty() || !learners_.empty())) {
        // 集群成员少了，qurum有可能变了，计算commit的方式也可能变了
        // 原本没有commit的日志现在可能已经可以算作commit了
        if (maybeCommit()) {
            bcastAppend();
        }
    }
}

void RaftFsm::promotePeer(const Peer& peer) {
    LOG_INFO("raft[%llu] promote peer: %s at term %lu", id_, peer.ToString().c_str(),
             term_);

    if (peer.type == PeerType::kLearner) {
        LOG_WARN("raft[%lu] can't promote learner to a learner: %s, ignore", id_,
                 peer.ToString().c_str());
        return;
    }

    auto it = learners_.find(peer.node_id);
    if (it == learners_.end()) {
        LOG_WARN("raft[%lu] promote learner doesn't exist: %s, ignore", id_,
                 peer.ToString().c_str());
        return;
    }

    auto old_peer = it->second->peer();
    // peer id不一致，不提升，防止旧日志重放
    if (old_peer.peer_id != peer.peer_id) {
        LOG_INFO("raft[%lu] ignore promote learner(inconsistent peer id), old: %s, "
                 "to promote: %s",
                 id_, old_peer.ToString().c_str(), peer.ToString().c_str());
        return;
    }

    learners_.erase(peer.node_id);
    auto replica = newReplica(peer, state_ == FsmState::kLeader);
    replicas_.emplace(peer.node_id, std::move(replica));

    if (peer.node_id == node_id_) {
        is_learner_ = false;
    }
}

int RaftFsm::quorum() const { return replicas_.size() / 2 + 1; }

void RaftFsm::send(MessagePtr& msg) {
    msg->set_id(id_);
    msg->set_from(node_id_);

    // LOCAL_MSG_PROP消息term为0
    // 其他类型如果已指定term则使用原msg的term
    if (msg->type() != pb::LOCAL_MSG_PROP && msg->term() == 0) {
        msg->set_term(term_);
    }

    sending_msgs_.push_back(msg);
}

void RaftFsm::reset(uint64_t term, bool is_leader) {
    if (term_ != term) {
        term_ = term;
        vote_for_ = 0;
    }

    leader_ = 0;
    election_elapsed_ = 0;
    heartbeat_elapsed_ = 0;
    votes_.clear();
    pending_conf_ = false;

    abortSendSnap();
    abortApplySnap();

    // reset non-learner replicas
    auto old_replicas = std::move(replicas_);
    for (const auto& r : old_replicas) {
        assert(r.second->peer().type == PeerType::kNormal);
        replicas_.emplace(r.first, newReplica(r.second->peer(), is_leader));
    }

    // reset learner replicas
    auto old_learners = std::move(learners_);
    for (auto& r : old_learners) {
        assert(r.second->peer().type == PeerType::kLearner);
        learners_.emplace(r.first, newReplica(r.second->peer(), is_leader));
    }

    if (is_leader) {
        rand_election_tick_ = sops_.election_tick - 1;
    } else {
        resetRandomizedElectionTimeout();
    }
}

void RaftFsm::resetRandomizedElectionTimeout() {
    rand_election_tick_ = random_func_();
    LOG_DEBUG("raft[%llu] election tick reset to %d", id_, rand_election_tick_);
}

bool RaftFsm::pastElectionTimeout() const {
    return election_elapsed_ >= rand_election_tick_;
}

void RaftFsm::abortSendSnap() {
    if (sending_snap_) {
        sending_snap_->Cancel();
        sending_snap_.reset();
    }
}

void RaftFsm::abortApplySnap() {
    if (applying_snap_) {
        applying_snap_->Cancel();
        applying_snap_.reset();
    }
}

bool RaftFsm::electable() const {
    // 还在成员内（非learner）
    return replicas_.find(node_id_) != replicas_.cend();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
