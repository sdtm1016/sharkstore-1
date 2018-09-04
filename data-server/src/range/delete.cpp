#include "range.h"

#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

bool Range::DeleteSubmit(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && (key.empty() || KeyInRange(key))) {
        auto ret = SubmitCmd(msg, req.header(), [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::Delete);
            cmd.set_allocated_delete_req(req.release_req());
        });
        return ret.ok();
    }
    return false;
}

bool Range::DeleteTry(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req) {
    std::shared_ptr<Range> rng = context_->FindRange(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    RANGE_LOG_DEBUG("Delete Try %" PRIu64, split_range_id_);

    return rng->DeleteSubmit(msg, req);
}

void Range::Delete(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("Delete begin");

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
        RANGE_LOG_WARN("Delete error: %s", err->message().c_str());

        auto resp = new kvrpcpb::DsKvDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
}

Status Range::ApplyDelete(const raft_cmdpb::Command &cmd) {
    Status ret;
    uint64_t affected_keys = 0;
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyDelete begin");

    auto &req = cmd.delete_req();
    auto btime = get_micro_second();

    do {
        auto &key = req.key();
        if (key.empty()) {
            auto &epoch = cmd.verify_epoch();

            if (!EpochIsEqual(epoch, err)) {
                RANGE_LOG_WARN("ApplyDelete error: %s", err->message().c_str());
                break;
            }
        } else {
            if (!KeyInRange(key, err)) {
                RANGE_LOG_WARN("ApplyDelete error: %s", err->message().c_str());
                break;
            }
        }

        ret = store_->DeleteRows(req, &affected_keys);
        context_->Statistics()->PushTime(HistogramType::kStore, get_micro_second() - btime);

        if (!ret.ok()) {
            RANGE_LOG_ERROR("ApplyDelete failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new kvrpcpb::DsKvDeleteResponse;
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
