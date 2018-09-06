#include "range.h"

#include "frame/sf_logger.h"
#include "server/range_server.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

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
    std::shared_ptr<Range> rng = context_->FindRange(split_range_id_);
    if (rng == nullptr) {
        return nullptr;
    }

    return rng->SelectResp(req);
}

void Range::Select(common::ProtoMessage *msg, kvrpcpb::DsSelectRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    auto ds_resp = new kvrpcpb::DsSelectResponse;
    auto header = ds_resp->mutable_header();

    RANGE_LOG_DEBUG("Select begin");

    do {
        if (!VerifyReadable(req.header().read_index(), err)) {
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
        context_->Statistics()->PushTime(HistogramType::kStore, etime - btime);

        if (etime - msg->begin_time > kTimeTakeWarnThresoldUSec) {
            RANGE_LOG_WARN("select takes too long(%" PRId64 " ms), sid=%" PRId64 ", msgid=%" PRId64,
                      (etime - msg->begin_time) / 1000, msg->session_id, msg->header.msg_id);
        }

        if (ret.ok()) {
            resp->set_code(0);
        } else {
            RANGE_LOG_WARN("Select from store error: %s", ret.ToString().c_str());

            resp->set_code(static_cast<int>(ret.code()));
        }

        if (key.empty() && !EpochIsEqual(epoch, err)) {
            ds_resp->clear_resp();

            RANGE_LOG_WARN("epoch change Select error: %s", err->message().c_str());
        }
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("Select error: %s", err->message().c_str());
    }

    RANGE_LOG_DEBUG("Select result: code=%d, rows=%d",
            (ds_resp->has_resp() ? ds_resp->resp().code() : 0),
            (ds_resp->has_resp() ? ds_resp->resp().rows_size() : 0));

    ds_resp->mutable_header()->set_apply_index(apply_index_);
    common::SetResponseHeader(req.header(), header, err);
    context_->SocketSession()->Send(msg, ds_resp);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
