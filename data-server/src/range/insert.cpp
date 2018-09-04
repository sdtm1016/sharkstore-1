#include "range.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

void Range::Insert(common::ProtoMessage *msg, kvrpcpb::DsInsertRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("Insert begin");

    if (!VerifyLeader(err)) {
        RANGE_LOG_WARN("Insert error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsInsertResponse;
        return SendError(msg, req.header(), resp, err);
    }

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsInsertResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    auto epoch = req.header().range_epoch();
    if (!EpochIsEqual(epoch, err)) {
        RANGE_LOG_WARN("Insert error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsInsertResponse;
        return SendError(msg, req.header(), resp, err);
    }
    auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
        cmd.set_cmd_type(raft_cmdpb::CmdType::Insert);
        cmd.set_allocated_insert_req(req.release_req());
    });
    if (!ret.ok()) {
        RANGE_LOG_ERROR("Insert raft submit error: %s", ret.ToString().c_str());

        auto resp = new kvrpcpb::DsInsertResponse;
        SendError(msg, req.header(), resp, RaftFailError());
    }
}

Status Range::ApplyInsert(const raft_cmdpb::Command &cmd) {
    Status ret;
    uint64_t affected_keys = 0;

    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyInsert begin");

    auto &req = cmd.insert_req();
    auto btime = get_micro_second();
    do {
        auto &epoch = cmd.verify_epoch();

        if (!EpochIsEqual(epoch, err)) {
            RANGE_LOG_WARN("ApplyInsert error: %s", err->message().c_str());
            break;
        }

        ret = store_->Insert(req, &affected_keys);
        auto etime = get_micro_second();
        context_->Statistics()->PushTime(HistogramType::kStore, etime - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyInsert failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            uint64_t len = 0;
            auto size = req.rows_size();

            for (int i = 0; i < size; i++) {
                auto keys_size = req.rows(i).key().size();
                auto values_size = req.rows(i).value().size();

                len += keys_size + values_size;
            }

            CheckSplit(len);
        }

    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsInsertResponse;
        resp->mutable_resp()->set_affected_keys(affected_keys);
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
