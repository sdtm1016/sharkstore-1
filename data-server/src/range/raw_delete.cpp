#include "range.h"

#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

bool Range::RawDeleteSubmit(common::ProtoMessage *msg,
                            kvrpcpb::DsKvRawDeleteRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && KeyInRange(key)) {
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::RawDelete);
            cmd.set_allocated_kv_raw_delete_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::RawDeleteTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req) {
    std::shared_ptr<Range> rng = context_->FindRange(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    return rng->RawDeleteSubmit(msg, req);
}

void Range::RawDelete(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("RawDelete begin");

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
            RANGE_LOG_WARN("RawDelete error: key empty");
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
        RANGE_LOG_WARN("RawDelete error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsKvRawDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyRawDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyRawDelete begin");

    auto btime = get_micro_second();
    auto &req = cmd.kv_raw_delete_req();

    do {
        if (!KeyInRange(req.key(), err)) {
            RANGE_LOG_WARN("ApplyRawDelete failed, epoch is changed");
            break;
        }

        ret = store_->Delete(req.key());
        context_->Statistics()->PushTime(HistogramType::kStore,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyRawDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvRawDeleteResponse;
        resp->mutable_resp()->set_code(ret.code());
        ReplySubmit(cmd, resp, err, btime);
    } else if (err != nullptr) {
        delete err;
    }
    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
