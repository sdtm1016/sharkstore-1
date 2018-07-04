#include "range.h"

#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

bool Range::RawDeleteSubmit(common::ProtoMessage *msg,
                            kvrpcpb::DsKvRawDeleteRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && KeyInRange(key)) {
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::RawDelete);
            cmd.set_allocated_kv_raw_delete_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::RawDeleteTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req) {
    std::shared_ptr<Range> rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    return rng->RawDeleteSubmit(msg, req);
}

void Range::RawDelete(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] RawDelete begin", meta_.id());

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvRawDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();

        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] RawDelete error: key empty", meta_.id());
            err = KeyNotInRange(key);
            break;
        }

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(key);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(key);
                break;
            }

            //! is_equal then retry raw delete
            if (!RawDeleteTry(msg, req)) {
                err = StaleEpochError(epoch);
            }

            break;
        }

        if (!RawDeleteSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] RawDelete error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvRawDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyRawDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    FLOG_DEBUG("range[%" PRIu64 "] ApplyRawDelete begin", meta_.id());

    auto &req = cmd.kv_raw_delete_req();

    do {
        if (!KeyInRange(req.key(), err)) {
            FLOG_WARN("ApplyRawDelete failed, epoch is changed");
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Delete(req.key());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyRawDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvRawDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
