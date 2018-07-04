#include "raft_impl.h"

#include <sstream>

#include "logger.h"
#include "raft_exception.h"
#include "raft_fsm.h"
#include "raft_types.h"

#include "snapshot/apply_task.h"
#include "snapshot/send_task.h"
#include "storage/storage.h"

namespace sharkstore {
namespace raft {
namespace impl {

RaftImpl::RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
                   const RaftContext& ctx)
    : sops_(sops), ops_(ops), ctx_(ctx), fsm_(new RaftFsm(sops, ops)) {
    initPublish();
}

RaftImpl::~RaftImpl() { Stop(); }

void RaftImpl::initPublish() {
    uint64_t leader = 0, term = 0;
    std::tie(leader, term) = fsm_->GetLeaderTerm();
    bulletin_board_.PublishLeaderTerm(leader, term);
    bulletin_board_.PublishPeers(fsm_->GetPeers());
    bulletin_board_.PublishStatus(fsm_->GetStatus());
}

Status RaftImpl::TryToLeader() {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }
    // 发送选举消息
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::LOCAL_MSG_HUP);
    msg->set_from(sops_.node_id);
    RecvMsg(msg);
    return Status::OK();
}

void RaftImpl::post(const std::function<void()>& f) {
    Work w;
    w.owner = ops_.id;
    w.stopped = &stopped_;
    w.f0 = f;
    ctx_.consensus_thread->post(w);
}

bool RaftImpl::tryPost(const std::function<void()>& f) {
    Work w;
    w.owner = ops_.id;
    w.stopped = &stopped_;
    w.f0 = f;
    return ctx_.consensus_thread->tryPost(w);
}

Status RaftImpl::Submit(std::string& cmd) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }

    if (ctx_.consensus_thread->submit(
            ops_.id, &stopped_,
            std::bind(&RaftImpl::Step, shared_from_this(), std::placeholders::_1), cmd)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }
}

Status RaftImpl::ChangeMemeber(const ConfChange& conf) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }

    std::string str;
    auto s = EncodeConfChange(conf, &str);
    if (!s.ok()) return s;

    auto msg = std::make_shared<pb::Message>();
    msg->set_type(pb::LOCAL_MSG_PROP);
    auto entry = msg->add_entries();
    entry->set_type(pb::ENTRY_CONF_CHANGE);
    entry->mutable_data()->swap(str);

    if (tryPost(std::bind(&RaftImpl::Step, shared_from_this(), msg))) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }

    return Status::OK();
}

void RaftImpl::Truncate(uint64_t index) {
    post(std::bind(&RaftImpl::truncate, shared_from_this(), index));
}

void RaftImpl::RecvMsg(MessagePtr msg) {
#ifdef FBASE_RAFT_TRACE_MSG
    if (msg->type() != pb::LOCAL_TICK) {
        LOG_DEBUG("recv msg type: %s from %llu, term: %llu at term: %llu",
                  pb::MessageType_Name(msg->type()).c_str(), msg->from(), msg->term(),
                  fsm_->term_);
    } else {
        LOG_DEBUG("recv msg type: LOCAL_TICK messages term: %llu at term: %llu",
                  msg->term(), fsm_->term_);
    }
#endif
    if (stopped_) return;

    if (!tryPost(std::bind(&RaftImpl::Step, shared_from_this(), msg))) {
        LOG_DEBUG("raft[%llu] discard a msg. type: %s from %llu, term: %llu", ops_.id,
                  pb::MessageType_Name(msg->type()).c_str(), msg->from(), msg->term());
    }
}

void RaftImpl::Tick(MessagePtr msg) {
    ++tick_count_;
    RecvMsg(msg);
}

void RaftImpl::Step(MessagePtr msg) {
    if (!fsm_->Validate(msg)) {
        LOG_DEBUG("raft[%lu] ignore invalidate msg type: %s from %llu, term: %llu",
                  ops_.id, pb::MessageType_Name(msg->type()).c_str(), msg->from(),
                  msg->term());
        return;
    }

    fsm_->Step(msg);
    fsm_->GetReady(&ready_);

    // 发送消息
    if (!ready_.msgs.empty()) sendMessages();

    // 发送快照
    if (ready_.send_snap) sendSnapshot();

    // 应用快照
    if (ready_.apply_snap) applySnapshot();

    // 应用
    apply();

    // 发布状态更新
    publish();

    // 持久化
    persist();
}

void RaftImpl::sendMessages() {
    for (auto m : ready_.msgs) {
        ctx_.msg_sender->SendMessage(m);
    }
}

void RaftImpl::sendSnapshot() {
    auto task = ready_.send_snap;
    assert(task != nullptr);

    task->SetReporter(std::bind(&RaftImpl::ReportSnapSendResult, shared_from_this(),
                                std::placeholders::_1, std::placeholders::_2));
    task->SetTransport(ctx_.msg_sender);

    SendSnapTask::Options send_opt;
    send_opt.max_size_per_msg = sops_.snapshot_options.max_size_per_msg;
    send_opt.wait_ack_timeout_secs = sops_.snapshot_options.ack_timeout_seconds;
    task->SetOptions(send_opt);

    auto s = ctx_.snapshot_manager->Dispatch(task);
    if (!s.ok()) {
        SnapResult result;
        result.status = s;
        ReportSnapSendResult(task->GetContext(), result);
    }
}

void RaftImpl::applySnapshot() {
    auto task = ready_.apply_snap;
    assert(task != nullptr);

    task->SetReporter(std::bind(&RaftImpl::ReportSnapApplyResult, shared_from_this(),
                                std::placeholders::_1, std::placeholders::_2));
    task->SetTransport(ctx_.msg_sender);

    ApplySnapTask::Options apply_opt;
    // TODO: use a config
    apply_opt.wait_data_timeout_secs = 10;
    task->SetOptions(apply_opt);

    auto s = ctx_.snapshot_manager->Dispatch(task);
    if (!s.ok()) {
        SnapResult result;
        result.status = s;
        ReportSnapApplyResult(task->GetContext(), result);
    }
}

// 应用
void RaftImpl::apply() {
    const auto& ents = ready_.committed_entries;
    for (const auto& e : ents) {
        if (e->type() == pb::ENTRY_CONF_CHANGE) {
            auto s = fsm_->applyConfChange(e);
            if (!s.ok()) {
                throw RaftException(std::string("apply confchange[") +
                                    std::to_string(e->index()) + "] error: " +
                                    s.ToString());
            }
            conf_changed_ = true;
        }
        if (sops_.apply_in_place) {
            // 同步应用
            smApply(e);
        } else {
            // 异步应用
            assert(ctx_.apply_thread != nullptr);
            Work w;
            w.owner = ops_.id;
            w.stopped = &stopped_;
            w.f0 = std::bind(&RaftImpl::smApply, shared_from_this(), e);
            ctx_.apply_thread->waitPost(w);
        }
    }
    if (!ents.empty()) {
        fsm_->raft_log_->appliedTo(fsm_->raft_log_->committed());
    }
}

// 持久化
void RaftImpl::persist() {
    auto hs = fsm_->GetHardState();
    bool hs_changed = prev_hard_state_.term() != hs.term() ||
                      prev_hard_state_.vote() != hs.vote() ||
                      prev_hard_state_.commit() != hs.commit();
    if (hs_changed) {
        prev_hard_state_ = hs;
    }
    auto s = fsm_->Persist(hs_changed);
    if (!s.ok()) throw RaftException(s);
}

void RaftImpl::publish() {
    // leader或term有变化，更新leader和term
    bool leader_changed = false;
    uint64_t leader = 0, term = 0;
    std::tie(leader, term) = fsm_->GetLeaderTerm();
    if (leader != bulletin_board_.Leader() || term != bulletin_board_.Term()) {
        bulletin_board_.PublishLeaderTerm(leader, term);
        leader_changed = true;
    }

    // 更新成员
    if (conf_changed_) {
        bulletin_board_.PublishPeers(fsm_->GetPeers());
    }

    // 每隔一定间隔更新RaftStatus
    if (conf_changed_ || leader_changed || tick_count_ % sops_.status_tick == 0) {
        bulletin_board_.PublishStatus(fsm_->GetStatus());
    }
    conf_changed_ = false;

    // 更新完状态最后通知外部
    if (leader_changed) {
        ops_.statemachine->OnLeaderChange(leader, term);
    }
}

void RaftImpl::ReportSnapSendResult(const SnapContext& ctx, const SnapResult& result) {
    if (result.status.ok()) {
        LOG_INFO("raft[%llu] send snapshot[uuid: %lu] to %lu finished. total "
                 "blocks: %d, bytes: %d",
                 ops_.id, ctx.uuid, ctx.to, result.blocks_count, result.bytes_count);
    } else {
        LOG_ERROR("raft[%llu] send snapshot[uuid: %llu] to %lu failed(%s). "
                  "sent blocks: %d, bytes: %d",
                  ops_.id, ctx.uuid, ctx.to, result.status.ToString().c_str(),
                  result.blocks_count, result.bytes_count);
    }

    // 通知本地leader
    MessagePtr resp(new pb::Message);
    resp->set_type(pb::LOCAL_SNAPSHOT_STATUS);
    resp->set_to(ctx.from);
    resp->set_from(ctx.to);
    resp->set_term(ctx.term);
    resp->set_reject(!result.status.ok());
    resp->mutable_snapshot()->set_uuid(ctx.uuid);

    post(std::bind(&RaftImpl::Step, shared_from_this(), resp));
}

void RaftImpl::ReportSnapApplyResult(const SnapContext& ctx, const SnapResult& result) {
    if (result.status.ok()) {
        LOG_INFO("raft[%llu] apply snapshot[uuid: %lu] to %lu finished. total "
                 "blocks: %d, bytes: %d",
                 ops_.id, ctx.uuid, ctx.to, result.blocks_count, result.bytes_count);
    } else {
        LOG_ERROR("raft[%llu] apply snapshot[uuid: %llu] to %lu failed(%s). "
                  "sent blocks: %d, bytes: %d",
                  ops_.id, ctx.uuid, ctx.to, result.status.ToString().c_str(),
                  result.blocks_count, result.bytes_count);
    }

    // 通知本地follower
    MessagePtr resp(new pb::Message);
    resp->set_type(pb::LOCAL_SNAPSHOT_STATUS);
    resp->set_to(sops_.node_id);
    resp->set_from(ctx.from);
    resp->set_term(ctx.term);
    resp->set_reject(!result.status.ok());
    resp->mutable_snapshot()->set_uuid(ctx.uuid);

    post(std::bind(&RaftImpl::Step, shared_from_this(), resp));
}

void RaftImpl::smApply(const EntryPtr& e) {
    auto s = fsm_->smApply(e);
    if (!s.ok()) {
        throw RaftException(std::string("statemachine apply entry[") +
                            std::to_string(e->index()) + "] error: " + s.ToString());
    }
}

void RaftImpl::Stop() { stopped_ = true; }

void RaftImpl::truncate(uint64_t index) {
    LOG_DEBUG("raft[%llu] truncate %llu", ops_.id, index);
    fsm_->TruncateLog(index);
}

Status RaftImpl::BackupLog() { return fsm_->BackupLog(); }

Status RaftImpl::Destroy() { return fsm_->DestroyLog(); }

}  // namespace impl
}  // namespace raft
} /* namespace sharkstore */
