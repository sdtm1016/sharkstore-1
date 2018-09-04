#include "range.h"

#include "server/range_server.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

kvrpcpb::KvRawGetResponse *Range::RawGetResp(const std::string &key) {
    if (is_leader_ && KeyInRange(key)) {
        auto resp = new kvrpcpb::KvRawGetResponse;
        auto ret = store_->Get(key, resp->mutable_value());
        if (ret.ok()) {
            resp->set_code(0);
        } else {
            resp->set_code(static_cast<int>(ret.code()));
        }
        return resp;
    }

    return nullptr;
}

kvrpcpb::KvRawGetResponse *Range::RawGetTry(const std::string &key) {
    auto rng = context_->FindRange(split_range_id_);
    if (rng == nullptr) {
        return nullptr;
    }

    return rng->RawGetResp(key);
}

void Range::RawGet(common::ProtoMessage *msg, kvrpcpb::DsKvRawGetRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    auto ds_resp = new kvrpcpb::DsKvRawGetResponse;
    auto header = ds_resp->mutable_header();

    RANGE_LOG_DEBUG("RawGet begin");

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();
        if (key.empty()) {
            RANGE_LOG_WARN("RawGet error: key empty");
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

            //! is_equal then retry raw get
            auto resp = RawGetTry(key);
            if (resp != nullptr) {
                ds_resp->set_allocated_resp(resp);
            } else {
                err = StaleEpochError(epoch);
            }

            break;
        }

        auto resp = ds_resp->mutable_resp();

        auto btime = get_micro_second();
        auto ret = store_->Get(req.req().key(), resp->mutable_value());
        context_->Statistics()->PushTime(HistogramType::kStore,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int>(ret.code()));
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("RawGet error: %s", err->message().c_str());
    }

    common::SetResponseHeader(req.header(), header, err);
    context_->SocketSession()->Send(msg, ds_resp);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
