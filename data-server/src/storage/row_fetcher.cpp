#include "row_fetcher.h"

#include <iostream>

#include "common/ds_encoding.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

static const size_t kIteratorTooManyKeys = 1000;

RowFetcher::RowFetcher(Store& s, const kvrpcpb::SelectRequest& req)
    : store_(s),
      decoder_(s.GetPrimaryKeys(), req.field_list(), req.where_filters()) {
    init(req.key(), req.scope());
}

RowFetcher::RowFetcher(Store& s, const kvrpcpb::DeleteRequest& req)
    : store_(s),
      decoder_(s.GetPrimaryKeys(), req.where_filters()) {
    init(req.key(), req.scope());
}

RowFetcher::~RowFetcher() { delete iter_; }

Status RowFetcher::Next(RowResult* result, bool* over) {
    if (!last_status_.ok()) {
        *over = true;
        return last_status_;
    }
    if (key_.empty()) {
        return nextScope(result, over);
    } else {
        return nextOneKey(result, over);
    }
}

void RowFetcher::init(const std::string& key, const ::kvrpcpb::Scope& scope) {
    if (!key.empty()) {
        key_ = key;
        return;
    }
    iter_ = store_.NewIterator(scope);
}

Status RowFetcher::nextOneKey(RowResult* result, bool* over) {
    assert(!key_.empty());

    // only read once
    if (iter_count_ > 0) {
        *over = true;
        return last_status_;
    }

    std::string buf;
    last_status_ = store_.Get(key_, &buf);
    iter_count_++;
    if (last_status_.code() == Status::kNotFound) {
        last_status_ = Status::OK();
        *over = true;
        return last_status_;
    } else if (!last_status_.ok()) {
        return last_status_;
    }

    matched_ = false;
    last_status_ = decoder_.DecodeAndFilter(key_, buf, result, &matched_);
    if (!last_status_.ok()) {
        return last_status_;
    }

    // if not matched, over
    *over = !matched_;
    return last_status_;
}

Status RowFetcher::nextScope(RowResult* result, bool* over) {
    assert(key_.empty());

    while (iter_->Valid()) {
        auto key = iter_->key();
        auto value = iter_->value();

        store_.addMetricRead(1, key.size() + value.size());
        // check iterator too many keys
        ++iter_count_;
        if (iter_count_ % kIteratorTooManyKeys == kIteratorTooManyKeys - 1) {
            FLOG_WARN("iterator too many keys(%lu), filters: %s",
                      iter_count_, decoder_.DebugString().c_str());
        }

        matched_ = false;
        last_status_ = decoder_.DecodeAndFilter(key, value, result, &matched_);
        if (!last_status_.ok()) {
            return last_status_;
        }

        FLOG_DEBUG("select decode key: %s, matched: %d", EncodeToHexString(key).c_str(), matched_);

        iter_->Next();
        if (matched_) {
            *over = false;
            return last_status_;
        }
    }

    last_status_ = iter_->status();
    *over = true;
    return last_status_;
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
