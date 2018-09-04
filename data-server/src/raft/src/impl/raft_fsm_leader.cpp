#include "raft_fsm.h"

#include <algorithm>
#include <sstream>
#include "logger.h"
#include "raft_exception.h"
#include "snapshot/send_task.h"

namespace sharkstore {
namespace raft {
namespace impl {

void RaftFsm::becomeLeader() {
    if (state_ == FsmState::kFollower) {
        throw RaftException(
            "[raft->becomeLeader]invalid transition [follower -> leader].");
    }

    step_func_ = std::bind(&RaftFsm::stepLeader, this, std::placeholders::_1);
    reset(term_, true);
    tick_func_ = std::bind(&RaftFsm::tickHeartbeat, this);
    leader_ = node_id_;
    state_ = FsmState::kLeader;

    std::vector<EntryPtr> ents;
    Status s = raft_log_->entries(raft_log_->committed() + 1, kNoLimit, &ents);
    if (!s.ok()) {
        throw RaftException(std::string("[raft->becomeLeader] unexpected error "
                                        "getting uncommitted entries:") +
                            s.ToString());
    }
    int nconf = numOfPendingConf(ents);
    if (nconf > 1) {
        throw RaftException("[raft->becomeLeader]unexpected double uncommitted config "
                            "entry");
    } else if (nconf == 1) {
        pending_conf_ = true;
    }

    // 添加一条空日志 尝试提交前任leader未提交的日志
    EntryPtr entry(new pb::Entry);
    entry->set_type(pb::ENTRY_NORMAL);
    entry->set_term(term_);
    entry->set_index(raft_log_->lastIndex() + 1);
    appendEntry(std::vector<EntryPtr>{entry});

    LOG_INFO("raft[%llu] become leader at term %llu", id_, term_);
}

void RaftFsm::stepLeader(MessagePtr& msg) {
    if (msg->type() == pb::LOCAL_MSG_PROP) {
        if (replicas_.find(node_id_) != replicas_.end() && msg->entries_size() > 0) {
            std::vector<EntryPtr> ents;
            takeEntries(msg, ents);
            // 保证同时只有一个ConfChange
            auto li = raft_log_->lastIndex();
            for (auto& entry : ents) {
                entry->set_index(++li);
                entry->set_term(term_);
                if (entry->type() == pb::ENTRY_CONF_CHANGE) {
                    if (pending_conf_) {
                        // 重新为空日志
                        entry->set_type(pb::ENTRY_NORMAL);
                        entry->clear_data();
                    }
                    pending_conf_ = true;
                }
            }
            appendEntry(ents);
            bcastAppend();
        }
        return;
    }

    // 其他类型的消息需要读取副本的进度
    auto replica = getReplica(msg->from());
    if (replica == nullptr) {
        LOG_WARN("raft[%llu] stepLeader no progress available for %llu", id_,
                 msg->from());
        return;
    }

    Replica& pr = *replica;
    pr.set_active();

    switch (msg->type()) {
        case pb::APPEND_ENTRIES_RESPONSE:
            if (msg->reject()) {
                LOG_DEBUG("raft[%llu] received msgApp "
                          "rejection(lastindex:%llu) from %llu for index %llu",
                          id_, msg->reject_hint(), msg->from(), msg->log_index());

                if (pr.maybeDecrTo(msg->log_index(), msg->reject_hint(), msg->commit())) {
                    if (pr.state() == ReplicaState::kReplicate) {
                        pr.becomeProbe();
                    }
                    sendAppend(msg->from(), pr);
                }
            } else {
                bool old_paused = pr.isPaused();
                if (pr.maybeUpdate(msg->log_index(), msg->commit())) {
                    switch (pr.state()) {
                        case ReplicaState::kProbe:
                            pr.becomeReplicate();
                            break;
                        case ReplicaState::kReplicate:
                            pr.inflight().freeTo(msg->log_index());
                            break;
                        case ReplicaState::kSnapshot:
                            if (pr.needSnapshotAbort()) {
                                LOG_INFO("raft[%llu] snapshot aborted, resumed "
                                         "sending replication to %llu",
                                         id_, msg->from());
                                // 取消快照发送
                                if (sending_snap_ &&
                                    sending_snap_->GetContext().to == msg->from()) {
                                    sending_snap_->Cancel();
                                    sending_snap_.reset();
                                }
                                pr.becomeProbe();
                            }
                            break;
                    }
                    if (maybeCommit()) {
                        bcastAppend();  // commit位置有更新，通知followers
                    } else if (old_paused) {
                        sendAppend(msg->from(), pr);
                    }
                }
            }
            return;

        case pb::HEARTBEAT_RESPONSE:
            pr.resume();
            if (pr.state() == ReplicaState::kReplicate && pr.inflight().full()) {
                pr.inflight().freeFirstOne();
            }
            // 进度没跟上，需要复制
            if (pr.match() < raft_log_->lastIndex() ||
                pr.committed() < raft_log_->committed()) {
                sendAppend(msg->from(), pr);
            }
            return;

        case pb::SNAPSHOT_ACK:
            if (sending_snap_ &&
                sending_snap_->GetContext().uuid == msg->snapshot().uuid()) {
                LOG_DEBUG("raft[%lu] recv snapshot[%lu] ack. seq=%ld, reject=%d", id_,
                          msg->snapshot().uuid(), msg->snapshot().seq(), msg->reject());

                auto s = sending_snap_->RecvAck(msg);
                if (!s.ok()) {
                    LOG_WARN("raft[%lu] ack[seq:%ld, reject:%d, from:%lu] snapshot[%lu] "
                             "failed: %s ",
                             id_, msg->snapshot().seq(), msg->reject(), msg->from(),
                             msg->snapshot().uuid(), s.ToString().c_str());
                }
            }

            return;

        case pb::LOCAL_SNAPSHOT_STATUS:
            if (!sending_snap_ ||
                sending_snap_->GetContext().uuid != msg->snapshot().uuid()) {
                return;
            }
            sending_snap_.reset();

            if (pr.state() != ReplicaState::kSnapshot) {
                return;
            }

            if (msg->reject()) {
                LOG_WARN("raft[%llu] send snapshot to [%llu] failed.", id_, msg->from());
                pr.snapshotFailure();
                pr.becomeProbe();
            } else {
                pr.becomeProbe();
                LOG_WARN("raft[%llu] send snapshot to [%llu] succeed, resumed "
                         "replication [%s]",
                         id_, msg->from(), pr.ToString().c_str());
            }

            pr.pause();
            return;
        default:
            return;
    }
}

void RaftFsm::tickHeartbeat() {
    ++heartbeat_elapsed_;

    if (state_ != FsmState::kLeader) {
        return;
    }

    // 增加副本的inactive_tick
    traverseReplicas([this](uint64_t node, Replica& pr) {
        if (node != node_id_) {
            pr.incr_inactive_tick();
        }
    });

    if (heartbeat_elapsed_ >= sops_.heartbeat_tick) {
        heartbeat_elapsed_ = 0;

        // 检查是否需要提升learner
        if (sops_.auto_promote_learner && !learners_.empty() && !pending_conf_) {
            checkCaughtUp();
        }
    }
}

bool RaftFsm::maybeCommit() {
    std::vector<uint64_t> matches;
    matches.reserve(replicas_.size());
    for (const auto& r : replicas_) {
        matches.push_back(r.second->match());
    }
    std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());

    uint64_t mid = matches[quorum() - 1];
    bool is_commit = raft_log_->maybeCommit(mid, term_);
    if (state_ == FsmState::kLeader) {
        auto it = replicas_.find(node_id_);
        if (it != replicas_.end()) {
            it->second->set_committed(raft_log_->committed());
        }
    }
    return is_commit;
}

void RaftFsm::bcastAppend() {
    traverseReplicas([this](uint64_t node, Replica& pr) {
        if (node != node_id_) this->sendAppend(node, pr);
    });
}

void RaftFsm::sendAppend(uint64_t to, Replica& pr) {
    assert(to == pr.peer().node_id);

    if (pr.isPaused()) {
        return;
    }

    if (pr.inactive_ticks() > sops_.inactive_tick) {
        pr.becomeProbe();
        pr.pause();
        return;
    }

    Status ts, es;
    uint64_t term = 0;
    std::vector<EntryPtr> ents;

    uint64_t fi = raft_log_->firstIndex();
    if (pr.next() >= fi) {
        ts = raft_log_->term(pr.next() - 1, &term);
        es = raft_log_->entries(pr.next(), sops_.max_size_per_msg, &ents);
    }

    // 需要发快照
    if (pr.next() < fi || !ts.ok() || !es.ok()) {
        LOG_INFO("raft[%llu] need snapshot to %llu[next:%llu], fi:%llu, log "
                 "error:%s-%s",
                 id_, to, pr.next(), fi, ts.ToString().c_str(), es.ToString().c_str());

        if (sending_snap_) {
            LOG_WARN("raft[%llu] sendAppend could not send snapshot to %llu(other "
                     "snapshot[%lu] is sending)",
                     id_, to, sending_snap_->GetContext().uuid);
        } else {
            uint64_t snap_index = 0;
            sending_snap_ = newSendSnapTask(to, &snap_index);
            pr.becomeSnapshot(snap_index);
        }
    } else {
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::APPEND_ENTRIES_REQUEST);
        msg->set_to(to);
        msg->set_log_index(pr.next() - 1);  // prev log index
        msg->set_log_term(term);            // prev log term
        msg->set_commit(raft_log_->committed());
        putEntries(msg, ents);

        if (msg->entries_size() > 0) {
            switch (pr.state()) {
                case ReplicaState::kReplicate: {
                    uint64_t last = msg->entries(msg->entries_size() - 1).index();
                    pr.update(last);
                    pr.inflight().add(last);
                    break;
                }
                case ReplicaState::kProbe:
                    pr.pause();
                    break;
                case ReplicaState::kSnapshot:
                    throw RaftException(
                        std::string("[repl->sendAppend][%v] is sending append "
                                    "in unhandled state ") +
                        ReplicateStateName(pr.state()));
            }
        }
        send(msg);
    }
}

void RaftFsm::appendEntry(const std::vector<EntryPtr>& ents) {
    LOG_DEBUG("raft[%llu] append log entry. index: %llu, size: %d", id_, ents[0]->index(),
              ents.size());

    raft_log_->append(ents);
    replicas_[node_id_]->maybeUpdate(raft_log_->lastIndex(), raft_log_->committed());
    maybeCommit();
}

static uint64_t unixNano() {
    auto now = std::chrono::system_clock::now();
    auto count = std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();
    return static_cast<uint64_t>(count);
}

std::shared_ptr<SendSnapTask> RaftFsm::newSendSnapTask(uint64_t to,
                                                       uint64_t* snap_index) {
    SnapContext snap_ctx;
    snap_ctx.id = id_;
    snap_ctx.to = to;
    snap_ctx.from = node_id_;
    snap_ctx.term = term_;
    snap_ctx.uuid = unixNano();

    pb::SnapshotMeta snap_meta;
    // 添加成员信息
    traverseReplicas([&snap_meta](uint64_t node, const Replica& pr) {
        auto p = snap_meta.add_peers();
        auto s = EncodePeer(pr.peer(), p);
        if (!s.ok()) {
            throw RaftException(std::string("create snapshot failed: ") + s.ToString());
        }
    });

    auto snap_data = sm_->GetSnapshot();
    if (snap_data == nullptr) {
        throw RaftException("raft->sendAppend failed to send snapshot, because "
                            "snapshot is unavailable");
    } else if (snap_data->ApplyIndex() < raft_log_->firstIndex() - 1) {
        std::ostringstream ss;
        ss << "raft->sendAppend[" << id_
           << "]failed to send snapshot, because snapshot is invalid(apply="
           << snap_data->ApplyIndex() << ", first=" << raft_log_->firstIndex() << ").";
        throw RaftException(ss.str());
    }

    // set meta index
    snap_meta.set_index(snap_data->ApplyIndex());
    // set meta term
    uint64_t snap_term = 0;
    auto status = raft_log_->term(snap_data->ApplyIndex(), &snap_term);
    if (!status.ok()) {
        std::ostringstream ss;
        ss << "[raft->sendAppend][" << id_ << "] failed to send snapshot to " << to
           << " because snapshot is unavailable, error is: " << status.ToString();
        throw RaftException(ss.str());
    }
    snap_meta.set_term(snap_term);

    // set user context
    std::string context;
    auto s = snap_data->Context(&context);
    if (!s.ok()) {
        throw RaftException(std::string("get snapshot user context failed: ") +
                            s.ToString());
    }
    snap_meta.set_context(std::move(context));

    *snap_index = snap_data->ApplyIndex();

    LOG_DEBUG("raft[%llu] sendAppend [firstindex: %llu, commit: %llu] sent "
              "snapshot[index: %llu, term: %llu] to [%llu]",
              id_, raft_log_->firstIndex(), raft_log_->committed(),
              snap_data->ApplyIndex(), snap_term, to);

    return std::make_shared<SendSnapTask>(snap_ctx, std::move(snap_meta), snap_data);
}

void RaftFsm::checkCaughtUp() {
    Peer max_peer;
    uint64_t max_match = 0;
    for (const auto& p : learners_) {
        const auto& pr = *p.second;
        if (pr.match() >= max_match) {
            max_match = pr.match();
            max_peer = pr.peer();
        }
    }

    auto lasti = raft_log_->lastIndex();
    auto precent_threshold = (sops_.promote_gap_percent * lasti) / 100;
    uint64_t final_threshold = std::max(sops_.promote_gap_threshold, precent_threshold);

    assert(lasti >= max_match);
    if (lasti - max_match < final_threshold) {
        LOG_INFO("raft[%lu] start promote learner %s [match:%lu, lasti:%lu] at term %lu.", id_,
                 max_peer.ToString().c_str(), max_match, lasti, term_);

        max_peer.type = PeerType::kNormal;
        ConfChange cc;
        cc.type = ConfChangeType::kPromote;
        cc.peer = max_peer;
        std::string str;
        auto s = EncodeConfChange(cc, &str);
        if (!s.ok()) {
            throw RaftException(std::string("promote caughtup learner failed:") +
                                s.ToString());
        }

        auto msg = std::make_shared<pb::Message>();
        msg->set_type(pb::LOCAL_MSG_PROP);
        auto entry = msg->add_entries();
        entry->set_type(pb::ENTRY_CONF_CHANGE);
        entry->mutable_data()->swap(str);
        Step(msg);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
