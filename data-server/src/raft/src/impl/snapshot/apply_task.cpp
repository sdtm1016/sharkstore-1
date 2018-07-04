#include "apply_task.h"

namespace sharkstore {
namespace raft {
namespace impl {

ApplySnapTask::ApplySnapTask(const SnapContext& context,
                             const std::shared_ptr<StateMachine>& sm)
    : SnapTask(context), sm_(sm) {}

ApplySnapTask::~ApplySnapTask() = default;

Status ApplySnapTask::RecvData(const MessagePtr& msg) {
    assert(msg->id() == GetContext().id);
    assert(msg->type() == pb::SNAPSHOT_REQUEST);

    if (msg->from() != GetContext().from) {
        return Status(Status::kInvalidArgument, "from",
                      std::to_string(GetContext().from));
    } else if (msg->term() != GetContext().term) {
        return Status(Status::kInvalidArgument, "term",
                      std::to_string(GetContext().term));
    } else if (msg->snapshot().uuid() != GetContext().uuid) {
        return Status(Status::kInvalidArgument, "uuid",
                      std::to_string(GetContext().uuid));
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (next_data_ != nullptr) {
            return Status(Status::kExisted, "prev block has not yet applied",
                          std::to_string(next_data_->snapshot().seq()));
        } else {
            next_data_ = msg;
        }
    }
    cv_.notify_one();

    return Status::OK();
}

void ApplySnapTask::run(SnapResult* result) {
    assert(sm_ != nullptr);
    assert(transport_ != nullptr);
    assert(opt_.wait_data_timeout_secs != 0);

    bool over = false;
    while (!over) {
        if (IsCanceled()) {
            result->status = Status(Status::kAborted, "canceled", "");
            return;
        }

        MessagePtr data;
        result->status = waitNextData(&data);
        if (!result->status.ok()) return;

        result->status = applyData(data, over);
        sendAck(data->snapshot().seq(), !result->status.ok());
        if (!result->status.ok()) {
            return;
        }
    }
}

Status ApplySnapTask::waitNextData(MessagePtr* data) {
    std::unique_lock<std::mutex> lock(mu_);
    if (cv_.wait_for(lock, std::chrono::seconds(opt_.wait_data_timeout_secs),
                     [this] { return next_data_ != nullptr || canceled_; })) {
        if (canceled_) {
            return Status(Status::kAborted, "canceled", "");
        } else {
            *data = std::move(next_data_);
            next_data_.reset();
            return Status::OK();
        }
    } else {
        return Status(Status::kTimedOut, "wait next data block",
                      std::to_string(prev_seq_ + 1));
    }
}

Status ApplySnapTask::applyData(MessagePtr data, bool& over) {
    auto snapshot = data->snapshot();

    // 检查快照块序号连续
    auto seq = snapshot.seq();
    if (seq != prev_seq_ + 1) {
        return Status(Status::kInvalidArgument, "discontinuous seq",
                      std::string("prev:") + std::to_string(prev_seq_) + ", msg:" +
                          std::to_string(seq));
    }

    // 第一条
    if (seq == 1) {
        auto s = sm_->ApplySnapshotStart(snapshot.meta().context());
        if (!s.ok()) return s;
        snap_index_ = snapshot.meta().index();
    }

    // 应用快照数据
    if (snapshot.datas_size() > 0) {
        std::vector<std::string> datas;
        datas.resize(static_cast<size_t>(snapshot.datas_size()));
        for (int i = 0; i < snapshot.datas_size(); ++i) {
            data->mutable_snapshot()->mutable_datas(i)->swap(datas[i]);
        }
        auto s = sm_->ApplySnapshotData(datas);
        if (!s.ok()) return s;
    }

    over = snapshot.final();
    // 最后一条
    if (over) {
        auto s = sm_->ApplySnapshotFinish(snap_index_);
        if (!s.ok()) return s;
    }

    ++prev_seq_;

    return Status::OK();
}

void ApplySnapTask::sendAck(int64_t seq, bool reject) {
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::SNAPSHOT_ACK);
    msg->set_id(GetContext().id);
    msg->set_from(GetContext().to);
    msg->set_to(GetContext().from);
    msg->set_term(GetContext().term);
    msg->set_reject(reject);
    msg->mutable_snapshot()->set_uuid(GetContext().uuid);
    msg->mutable_snapshot()->set_seq(seq);

    transport_->SendMessage(msg);

    // TODO: log
    // LOG_DEBUG("raft[%lu] send snapshot[%lu] ack to %lu. seq=%ld, reject=%d", id_,
    // ack->snapshot().uuid(), ack->to(),
    // ack->snapshot().seq(), ack->reject());
}

void ApplySnapTask::Cancel() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        canceled_ = true;
    }

    cv_.notify_one();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
