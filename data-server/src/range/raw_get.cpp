#include "range.h"

#include "server/range_server.h"

namespace sharkstore {
namespace dataserver {
namespace range {

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
    auto rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return nullptr;
    }

    return rng->RawGetResp(key);
}

void Range::RawGet(common::ProtoMessage *msg, kvrpcpb::DsKvRawGetRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new kvrpcpb::DsKvRawGetResponse;
    auto header = ds_resp->mutable_header();

    FLOG_DEBUG("range[%" PRIu64 "] RawGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();
        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] RawGet error: key empty", meta_.id());
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
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int>(ret.code()));
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] RawGet error: %s", meta_.id(),
                  err->message().c_str());
    }

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
