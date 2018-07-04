#include "row_fetcher.h"

#include <iostream>

#include "common/ds_encoding.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

RowFetcher::RowFetcher(Store& s, const kvrpcpb::SelectRequest& req)
    : store_(s),
      iter_(nullptr),
      decoder_(s.GetPrimaryKeys(), req.field_list(), req.where_filters()),
      matched_(false),
      count_(0) {
    init(req.key(), req.scope());
}

RowFetcher::RowFetcher(Store& s, const kvrpcpb::DeleteRequest& req)
    : store_(s),
      iter_(nullptr),
      decoder_(s.GetPrimaryKeys(), req.where_filters()),
      matched_(false),
      count_(0) {
    init(req.key(), req.scope());
}

RowFetcher::~RowFetcher() { delete iter_; }

Status RowFetcher::Next(RowResult* result, bool* over) {
    if (!status_.ok()) {
        *over = true;
        return status_;
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

    if (count_ > 0) {
        *over = true;
        return status_;
    }

    std::string buf;
    status_ = store_.Get(key_, &buf);
    if (status_.code() == Status::kNotFound) {
        status_ = Status::OK();
        *over = true;
        return status_;
    } else if (!status_.ok()) {
        return status_;
    }

    matched_ = false;
    status_ = decoder_.DecodeAndFilter(key_, buf, result, &matched_);
    if (!status_.ok()) {
        return status_;
    }
    if (!matched_) {
        *over = true;
    } else {
        *over = false;
        ++count_;
    }
    return status_;
}

Status RowFetcher::nextScope(RowResult* result, bool* over) {
    assert(key_.empty());

    while (iter_->Valid()) {
        auto key = iter_->key();
        auto value = iter_->value();
        matched_ = false;

        status_ = decoder_.DecodeAndFilter(key, value, result, &matched_);

        FLOG_DEBUG("select decode key: %s, matched: %d",
                   EncodeToHexString(key).c_str(), matched_);

        store_.addMetricRead(1, key.size() + value.size());

        if (!status_.ok()) {
            return status_;
        }
        iter_->Next();
        if (matched_) {
            *over = false;
            ++count_;
            return status_;
        }
    }

    status_ = iter_->status();
    *over = true;
    return status_;
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
