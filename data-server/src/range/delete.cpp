#include "range.h"

#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

bool Range::DeleteSubmit(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && (key.empty() || KeyInRange(key))) {
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Delete);
            cmd.set_allocated_delete_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::DeleteTry(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req) {
    std::shared_ptr<Range> rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    FLOG_DEBUG("range[%" PRIu64 "] Delete Try ", split_range_id_);

    return rng->DeleteSubmit(msg, req);
}

void Range::Delete(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] Delete begin", meta_.id());

    if (!CheckWriteable()) {
        auto resp = new kvrpcpb::DsKvDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();
        auto epoch = req.header().range_epoch();

        bool is_equal = EpochIsEqual(epoch);

        if (key.empty()) {
            if (!is_equal) {
                err = StaleEpochError(epoch);
                break;
            }
        } else {
            bool in_range = KeyInRange(key);
            if (!in_range) {
                if (is_equal) {
                    err = KeyNotInRange(key);
                    break;
                }

                //! is_equal then retry delete
                if (!DeleteTry(msg, req)) {
                    err = StaleEpochError(epoch);
                }

                break;
            }
        }

        if (!DeleteSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] Delete error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new kvrpcpb::DsKvDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;

    FLOG_DEBUG("ApplyDelete begin");

    auto &req = cmd.delete_req();

    do {
        auto &key = req.key();
        if (key.empty()) {
            auto epoch = cmd.verify_epoch();

            if (!EpochIsEqual(epoch, err)) {
                FLOG_WARN("Range %" PRIu64 "  ApplyDelete error: %s", meta_.id(),
                          err->message().c_str());
                break;
            }
        } else {
            if (!KeyInRange(key, err)) {
                FLOG_WARN("range[%" PRIu64 "] ApplyDelete error: %s", meta_.id(),
                          err->message().c_str());
                break;
            }
        }

        auto btime = get_micro_second();
        ret = store_->DeleteRows(req, &affected_keys);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), affected_keys, err);
    } else if (err != nullptr) {
        delete err;
    }

    return ret;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
