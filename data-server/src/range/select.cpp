#include "range.h"

#include "frame/sf_logger.h"
#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

kvrpcpb::SelectResponse *Range::SelectResp(const kvrpcpb::DsSelectRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && (key.empty() || KeyInRange(key))) {
        auto resp = new kvrpcpb::SelectResponse;
        auto ret = store_->Select(req.req(), resp);
        if (ret.ok()) {
            resp->set_code(0);
        } else {
            resp->set_code(static_cast<int>(ret.code()));
        }

        return resp;
    }

    return nullptr;
}

kvrpcpb::SelectResponse *Range::SelectTry(const kvrpcpb::DsSelectRequest &req) {
    std::shared_ptr<Range> rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return nullptr;
    }

    return rng->SelectResp(req);
}

void Range::Select(common::ProtoMessage *msg, kvrpcpb::DsSelectRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new kvrpcpb::DsSelectResponse;
    auto header = ds_resp->mutable_header();

    FLOG_DEBUG("range[%" PRIu64 "] Select begin", meta_.id());

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
            if (!KeyInRange(key)) {
                if (is_equal) {
                    err = KeyNotInRange(key);
                    break;
                }

                //! is_equal then retry select
                auto resp = SelectTry(req);
                if (resp != nullptr) {
                    ds_resp->set_allocated_resp(resp);
                } else {
                    err = StaleEpochError(epoch);
                }

                break;
            }
        }

        auto resp = ds_resp->mutable_resp();

        auto btime = get_micro_second();
        auto ret = store_->Select(req.req(), resp);
        auto etime = get_micro_second();
        context_->run_status->PushTime(monitor::PrintTag::Store, etime - btime);

        if (etime - msg->begin_time > kTimeTakeWarnThresoldUSec) {
            FLOG_WARN("range[%lu] select takes too long(%ld ms), sid=%ld, msgid=%ld",
                      meta_.id(), (etime - msg->begin_time) / 1000, msg->session_id,
                      msg->header.msg_id);
        }

        if (ret.ok()) {
            resp->set_code(0);
        } else {
            FLOG_WARN("range[%" PRIu64 "] Select from store error: %s", meta_.id(),
                      ret.ToString().c_str());

            resp->set_code(static_cast<int>(ret.code()));
        }

        if (key.empty() && !EpochIsEqual(epoch, err)) {
            ds_resp->clear_resp();

            FLOG_WARN("range[%" PRIu64 "] epoch change Select error: %s", meta_.id(),
                      err->message().c_str());
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] Select error: %s", meta_.id(),
                  err->message().c_str());
    }

    FLOG_DEBUG("range[%" PRIu64 "] Select result: code=%d, rows=%d", meta_.id(),
               (ds_resp->has_resp() ? ds_resp->resp().code() : 0),
               (ds_resp->has_resp() ? ds_resp->resp().rows_size() : 0));

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
