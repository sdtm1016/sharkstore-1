#include "raft_fsm.h"

#include "logger.h"
#include "storage/storage.h"
#include "snapshot/apply_task.h"

namespace sharkstore {
namespace raft {
namespace impl {

void RaftFsm::becomeFollower(uint64_t term, uint64_t leader) {
    step_func_ = std::bind(&RaftFsm::stepFollower, this, std::placeholders::_1);
    reset(term, false);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    leader_ = leader;
    state_ = FsmState::kFollower;

    LOG_INFO("raft[%llu] become follwer at term %llu", id_, term_);
}

void RaftFsm::stepFollower(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_PROP:
            if (leader_ == 0) {
                LOG_DEBUG("raft[%llu] no leader at term %llu; dropping proposal.", id_,
                          term_);
            } else {
                // 转发给leader
                msg->set_to(leader_);
                send(msg);
            }
            return;

        case pb::APPEND_ENTRIES_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            handleAppendEntries(msg);
            return;

        case pb::HEARTBEAT_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            return;

        case pb::SNAPSHOT_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            handleSnapshot(msg);
            return;

        case pb::LOCAL_SNAPSHOT_STATUS:
            if (!applying_snap_ || applying_snap_->GetContext().uuid != msg->snapshot().uuid()) {
                return;
            }

            if (!msg->reject()) {
                auto s = restore(applying_meta_);
                if (!s.ok()) {
                    LOG_ERROR("raft[%lu] restore snapshot[%lu] failed(%s) at term %lu.",
                              id_, applying_snap_->GetContext().uuid, s.ToString().c_str(), term_);
                } else {
                    // 通知leader应用快照成功
                    MessagePtr resp(new pb::Message);
                    resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
                    resp->set_to(msg->from());
                    resp->set_log_index(raft_log_->lastIndex());
                    resp->set_commit(raft_log_->committed());
                    send(resp);
                }
            }
            applying_snap_.reset();
            return;

        default:
            return;
    }
}

void RaftFsm::tickElection() {
    // 检查是否还在成员内
    if (!electable()) {
        election_elapsed_ = 0;
        return;
    }

    ++election_elapsed_;
    // 选举超时
    if (pastElectionTimeout()) {
        election_elapsed_ = 0;
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::LOCAL_MSG_HUP);
        msg->set_from(node_id_);
        Step(msg);
    }
}

void RaftFsm::handleAppendEntries(MessagePtr& msg) {
    MessagePtr resp_msg(new pb::Message);
    resp_msg->set_type(pb::APPEND_ENTRIES_RESPONSE);
    resp_msg->set_to(msg->from());
    resp_msg->set_reject(false);

    // 复制的日志不能小于commit位置，如果小于告诉leader日志已经到了commit的位置，
    // 请发送大于commit位置的日志
    if (msg->log_index() < raft_log_->committed()) {
        resp_msg->set_log_index(raft_log_->committed());
        resp_msg->set_commit(raft_log_->committed());
        send(resp_msg);
        return;
    }

    std::vector<EntryPtr> ents;
    takeEntries(msg, ents);
    uint64_t last_index = 0;
    if (raft_log_->maybeAppend(msg->log_index(), msg->log_term(), msg->commit(), ents,
                               &last_index)) {
        resp_msg->set_log_index(last_index);
        resp_msg->set_commit(raft_log_->committed());
        send(resp_msg);
    } else {
        LOG_DEBUG("raft[%llu] [logterm:%llu, index:%llu] rejected msgApp from "
                  "%llu[logterm:%llu, index:%llu]",
                  id_, raft_log_->lastTerm(), raft_log_->lastIndex(), msg->from(),
                  msg->log_term(), msg->log_index());

        resp_msg->set_log_index(msg->log_index());
        resp_msg->set_commit(raft_log_->committed());
        resp_msg->set_reject(true);
        resp_msg->set_reject_hint(raft_log_->lastIndex());
        send(resp_msg);
    }
}

void RaftFsm::handleSnapshot(MessagePtr& msg) {
    auto s = applySnapshot(msg);
    if (!s.ok()) {
        LOG_ERROR("raft[%lu] apply snapshot[%lu:%lu] from %lu at term %lu failed: %s", id_, msg->snapshot().uuid(),
                  msg->snapshot().seq(), msg->from(), term_, s.ToString().c_str());

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::SNAPSHOT_ACK);
        resp->set_to(msg->from());
        resp->set_reject(true);
        resp->mutable_snapshot()->set_uuid(msg->snapshot().uuid());
        resp->mutable_snapshot()->set_seq(msg->snapshot().seq());
        send(resp);
    }
}

Status RaftFsm::applySnapshot(MessagePtr& msg) {
    if (applying_snap_) {
        bool reject = false;
        if (applying_snap_->GetContext().uuid != msg->snapshot().uuid()) {
            return Status(Status::kExisted, "othre snapshot is applying", std::to_string(applying_snap_->GetContext().uuid));
        } else {
            auto s = applying_snap_->RecvData(msg);
            if (!s.ok()) {
                return s;
            }
        }
        return Status::OK();
    }

    const auto& snapshot = msg->snapshot();
    // uuid不能为0
    if (snapshot.uuid() == 0) {
        return Status(Status::kInvalidArgument, "uuid", "0");
    }
    if (!snapshot.has_meta()) {
        return Status(Status::kInvalidArgument, "meta", "missing");
    }
    if (snapshot.seq() != 1) {
        return Status(Status::kInvalidArgument, "seq", "invalid first seq");
    }

    // 过时的快照
    if (!checkSnapshot(snapshot.meta())) {
        LOG_WARN("raft[%llu] [commit: %llu] ignored snapshot [index: %llu, "
                 "term: %llu].",
                 id_, raft_log_->committed(), snapshot.meta().index(), snapshot.meta().term());

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
        resp->set_to(msg->from());
        resp->set_log_index(raft_log_->lastIndex());
        resp->set_commit(raft_log_->committed());
        send(resp);
        return Status::OK();
    }

    SnapContext ctx;
    ctx.uuid = snapshot.uuid();
    ctx.from = msg->from();
    ctx.id = id_;
    ctx.term = term_;
    ctx.to = node_id_;

    // new apply task
    applying_snap_ = std::make_shared<ApplySnapTask>(ctx, sm_);
    applying_meta_ = snapshot.meta();
    return applying_snap_->RecvData(msg);
}

bool RaftFsm::checkSnapshot(const pb::SnapshotMeta& meta) {
    if (meta.index() <= raft_log_->committed()) {
        // 快照的数据都已经commit过了
        return false;
    } else if (raft_log_->matchTerm(meta.index(), meta.term())) {
        // 快照的数据日志里都有
        raft_log_->commitTo(meta.index());
        return false;
    } else {
        return true;
    }
}

Status RaftFsm::restore(const pb::SnapshotMeta& meta) {
    LOG_WARN("raft[%llu] [commit:%llu, lastindex:%llu, lastterm:%llu] starts "
             "to restore snapshot [index:%llu, term:%llu], peers: %d",
             id_, raft_log_->committed(), raft_log_->lastIndex(), raft_log_->lastTerm(),
             meta.index(), meta.term(), meta.peers_size());

    auto s = storage_->ApplySnapshot(meta);
    if (!s.ok()) return s;

    raft_log_->restore(meta.index());

    // restore all replicas
    replicas_.clear();
    learners_.clear();
    for (int i = 0; i < meta.peers_size(); ++i) {
        Peer peer;
        auto s = DecodePeer(meta.peers(i), &peer);
        if (!s.ok()) {
            return s;
        }

        if (peer.node_id == node_id_) {
            if (!is_learner_ && peer.type == PeerType::kLearner) {
                return Status(Status::kInvalidArgument, "restore meta",
                              "counld degrade from a normal peer to leadrner");
            }
            // promote self from snapshot
            if (is_learner_ && peer.type != PeerType::kLearner) {
                LOG_INFO("raft[%lu] promote self from snapshot", id_);
                is_learner_ = false;
            }
        }

        if (peer.type == PeerType::kLearner) {
            learners_.emplace(peer.node_id, newReplica(peer, false));
        } else {
            replicas_.emplace(peer.node_id, newReplica(peer, false));
        }
    }
    return Status::OK();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
