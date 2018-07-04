#include "raft_fsm.h"

#include <functional>
#include "logger.h"
#include "raft_exception.h"

namespace sharkstore {
namespace raft {
namespace impl {

void RaftFsm::becomeCandidate() {
    if (state_ == FsmState::kLeader) {
        throw RaftException(
            "[raft->becomeCandidate] invalid transition [leader -> candidate]");
    }

    step_func_ = std::bind(&RaftFsm::stepCandidate, this, std::placeholders::_1);
    reset(term_ + 1, false);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    vote_for_ = node_id_;
    state_ = FsmState::kCandidate;

    LOG_INFO("raft[%llu] become candidate at term %llu", id_, term_);
}

void RaftFsm::becomePreCandidate() {
    if (state_ == FsmState::kLeader) {
        throw RaftException("invalid transition[leader -> pre-candidate]");
    }

    step_func_ = std::bind(&RaftFsm::stepCandidate, this, std::placeholders::_1);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    state_ = FsmState::kPreCandidate;
    votes_.clear();

    LOG_INFO("raft[%llu] become pre-candidate at term %llu", id_, term_);
}

void RaftFsm::stepCandidate(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_PROP:
            LOG_DEBUG("raft[%llu] no leader at term %llu; dropping proposal.", id_,
                      term_);
            return;

        case pb::APPEND_ENTRIES_REQUEST:
            becomeFollower(term_, msg->from());
            handleAppendEntries(msg);
            return;

        case pb::HEARTBEAT_REQUEST:
            becomeFollower(term_, msg->from());
            return;

        case pb::VOTE_RESPONSE:
        case pb::PRE_VOTE_RESPONSE: {
            bool pre = false;
            if (msg->type() == pb::PRE_VOTE_RESPONSE) {
                if (state_ != FsmState::kPreCandidate) return;
                pre = true;
            } else {
                if (state_ != FsmState::kCandidate) return;
            }

            int grants = poll(pre, msg->from(), !msg->reject());
            int rejects = votes_.size() - grants;

            LOG_INFO("raft[%llu] %s [q:%d] has received %d votes and %d vote "
                     "rejections",
                     id_, (pre ? "pre-campaign" : "campaign"), quorum(), grants, rejects);

            if (grants == quorum()) {  // 收到大多数投票
                if (pre) {
                    campaign(false);
                } else {
                    becomeLeader();
                    bcastAppend();
                }
            } else if (rejects == quorum()) {  // 大多数都拒绝了
                becomeFollower(term_, 0);
            }
            return;
        }

        default:
            return;
    }
}

void RaftFsm::campaign(bool pre) {
    if (pre) {
        becomePreCandidate();
    } else {
        becomeCandidate();
    }

    // 可能只有一个成员
    if (poll(pre, node_id_, true) == quorum()) {
        if (pre) {
            campaign(false);
        } else {
            becomeLeader();
        }
        return;
    }

    // 发送投票请求
    uint64_t li = 0, lt = 0;
    raft_log_->lastIndexAndTerm(&li, &lt);
    for (const auto& r : replicas_) {
        if (r.first == node_id_) continue;

        LOG_INFO("raft[%llu] %s: [logterm: %llu, index: %llu] sent vote "
                 "request to %llu at term %llu.",
                 id_, (pre ? "pre-campaign" : "campaign"), lt, li, r.first, term_);

        MessagePtr msg(new pb::Message);
        msg->set_type(pre ? pb::PRE_VOTE_REQUEST : pb::VOTE_REQUEST);
        // PreVote请求的term是当前的term+1，因为preCandidate状态不增加term
        msg->set_term(pre ? (term_ + 1) : term_);
        msg->set_to(r.first);
        msg->set_log_index(li);
        msg->set_log_term(lt);
        send(msg);
    }
}

int RaftFsm::poll(bool pre, uint64_t node_id, bool vote) {
    LOG_INFO("raft[%llu] received %s %s from %llu at term %llu", id_,
             (pre ? "pre-vote" : "vote"), (vote ? "granted" : "rejected"), node_id,
             term_);

    votes_.emplace(node_id, vote);

    int grants = 0;
    for (const auto& kv : votes_) {
        if (kv.second) ++grants;
    }
    return grants;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
