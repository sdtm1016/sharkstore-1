#include "iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Iterator::Iterator(rocksdb::Iterator* it, const std::string& start,
                   const std::string& limit)
    : rit_(it), limit_(limit) {
    assert(!start.empty());
    assert(!limit.empty());
    rit_->Seek(start);
}

Iterator::~Iterator() { delete rit_; }

bool Iterator::Valid() { return rit_->Valid() && (key() < limit_); }

void Iterator::Next() { rit_->Next(); }

Status Iterator::status() {
    if (!rit_->status().ok()) {
        return Status(Status::kIOError, rit_->status().ToString(), "");
    }
    return Status::OK();
}

std::string Iterator::key() { return rit_->key().ToString(); }

std::string Iterator::value() { return rit_->value().ToString(); }

uint64_t Iterator::key_size() { return rit_->key().size(); }

uint64_t Iterator::value_size() { return rit_->value().size(); }

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
