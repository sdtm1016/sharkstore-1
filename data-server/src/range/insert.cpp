#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::Insert(common::ProtoMessage *msg, kvrpcpb::DsInsertRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait,
                                   btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] Insert begin", meta_.id());

    if (!VerifyLeader(err)) {
        FLOG_WARN("range[%" PRIu64 "] Insert error: %s", meta_.id(),
                  err->message().c_str());

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
        FLOG_WARN("range[%" PRIu64 "] Insert error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsInsertResponse;
        return SendError(msg, req.header(), resp, err);
    }
    auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
        cmd.set_cmd_type(raft_cmdpb::CmdType::Insert);
        cmd.set_allocated_insert_req(req.release_req());
    });
    if (!ret.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] Insert raft submit error: %s",
                   meta_.id(), ret.ToString().c_str());

        auto resp = new kvrpcpb::DsInsertResponse;
        SendError(msg, req.header(), resp, RaftFailError());
    }
}

Status Range::ApplyInsert(const raft_cmdpb::Command &cmd) {
    Status ret;
    uint64_t affected_keys = 0;

    errorpb::Error *err = nullptr;

    FLOG_DEBUG("Range %" PRIu64 " ApplyInsert begin", meta_.id());

    auto &req = cmd.insert_req();
    do {
        auto epoch = cmd.verify_epoch();

        if (!EpochIsEqual(epoch, err)) {
            FLOG_WARN("Range %" PRIu64 "  ApplyInsert error: %s", meta_.id(),
                      err->message().c_str());
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Insert(req, &affected_keys);
        auto etime = get_micro_second();
        context_->run_status->PushTime(monitor::PrintTag::Store, etime - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyInsert failed, code:%d, msg:%s", ret.code(),
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
        SendResponse(resp, cmd, static_cast<int>(ret.code()), affected_keys,
                     err);

    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
