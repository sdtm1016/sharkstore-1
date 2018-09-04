#include "submit.h"

#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "proto/gen/funcpb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace range {

SubmitContext::SubmitContext(const kvrpcpb::RequestHeader &req_header,
        raft_cmdpb::CmdType type, common::ProtoMessage *msg) :
    cluster_id_(req_header.cluster_id()),
    trace_id_(req_header.trace_id()),
    create_time_(get_micro_second()),
    type_(type),
    msg_(msg) {
}

SubmitContext::~SubmitContext() {
    delete msg_;
}

void SubmitContext::SendTimeout(common::SocketSession* session) {
    auto err = new errorpb::Error;
    err->set_message("request timeout");
    err->mutable_timeout();
    switch (type_) {
        case raft_cmdpb::CmdType::RawPut:
            Reply(session, new kvrpcpb::DsKvRawPutResponse, err);
            break;
        case raft_cmdpb::CmdType::RawDelete:
            Reply(session, new kvrpcpb::DsKvRawDeleteResponse, err);
            break;
        case raft_cmdpb::CmdType::Insert:
            Reply(session, new kvrpcpb::DsInsertResponse, err);
            break;
        case raft_cmdpb::CmdType::Delete:
            Reply(session, new kvrpcpb::DsDeleteResponse, err);
            break;
        default:
            FLOG_ERROR("SubmitContext::SendTimeout: unknown cmd type: %d", static_cast<int>(type_));
            delete err;
    }
}

void SubmitContext::CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs) {
    auto take = get_micro_second() - msg_->begin_time;
    if (take > thresold_usecs) {
        auto method = funcpb::FunctionID_Name(static_cast<funcpb::FunctionID>(msg_->header.func_id));
        FLOG_WARN("range[%" PRIu64 "] %s takes too long(%" PRId64 " ms), sid=%" PRId64 ", msgid=%" PRId64,
                rangeID, method.c_str(), take / 1000, msg_->session_id, msg_->header.msg_id);
    }
}


uint64_t SubmitQueue::GetSeq() {
    std::lock_guard<std::mutex> lock(mu_);
    return ++seq_;
}

uint64_t SubmitQueue::Add(const kvrpcpb::RequestHeader& req_header,
             raft_cmdpb::CmdType type, common::ProtoMessage *msg) {
    SubmitContextPtr ctx(new SubmitContext(req_header, type, msg));

    std::lock_guard<std::mutex> lock(mu_);
    ctx_map_.emplace(++seq_, std::move(ctx));
    expire_que_.emplace(msg->expire_time, seq_);
    return seq_;
}

std::unique_ptr<SubmitContext> SubmitQueue::Remove(uint64_t seq_id) {
    std::unique_ptr<SubmitContext> ret;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ctx_map_.find(seq_id);
    if (it != ctx_map_.end()) {
        ret = std::move(it->second);
        ctx_map_.erase(it);
    }
    return ret;
}

std::vector<uint64_t> SubmitQueue::GetExpired(size_t max_count) {
    std::vector<uint64_t> result;
    auto now = getticks();
    {
        std::lock_guard<std::mutex> lock(mu_);
        while (!expire_que_.empty() && result.size() < max_count) {
            auto& p = expire_que_.top();
            if (p.first < now) {
                result.push_back(p.second);
                expire_que_.pop();
            } else {
                break;
            }
        }
    }
    return result;
}

size_t SubmitQueue::Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return ctx_map_.size();
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
