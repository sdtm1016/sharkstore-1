#include "meta_store.h"

#include <errno.h>
#include <rocksdb/write_batch.h>
#include <memory>

#include "base/util.h"
#include "common/ds_encoding.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

MetaStore::MetaStore(const std::string &path) : path_(path) {
    write_options_.sync = true;
}

MetaStore::~MetaStore() { delete db_; }

Status MetaStore::Open() {
    int ret = MakeDirAll(path_, 0755);
    if (ret != 0) {
        return Status(Status::kIOError, "create meta store directory",
                      strErrno(errno));
    }

    rocksdb::Options ops;
    ops.create_if_missing = true;
    auto s = rocksdb::DB::Open(ops, path_, &db_);
    if (!s.ok()) {
        return Status(Status::kIOError, "open meta store db", s.ToString());
    }

    return Status::OK();
}

Status MetaStore::SaveNodeID(uint64_t node_id) {
    auto ret = db_->Put(write_options_, kNodeIDKey, std::to_string(node_id));
    if (ret.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, ret.ToString(), "meta save node");
    }
}

Status MetaStore::GetNodeID(uint64_t *node_id) {
    std::string value;
    auto ret = db_->Get(rocksdb::ReadOptions(), kNodeIDKey, &value);
    if (ret.ok()) {
        try {
            *node_id = std::stoull(value);
        } catch (std::exception &e) {
            return Status(Status::kCorruption, "invalid node_id",
                          EncodeToHexString(value));
        }
        return Status::OK();
    } else if (ret.IsNotFound()) {
        *node_id = 0;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta load node", ret.ToString());
    }
}

Status MetaStore::GetAllRange(std::vector<std::string> &meta_ranges) {
    std::shared_ptr<rocksdb::Iterator> it(
        db_->NewIterator(rocksdb::ReadOptions()));

    for (it->Seek(kRangeMetaPrefix);
         it->Valid() && it->key().starts_with(kRangeMetaPrefix); it->Next()) {
        meta_ranges.push_back(std::move(it->value().ToString()));
    }

    if (it->status().ok()) {
        return Status::OK();
    }
    return Status(Status::kIOError, it->status().ToString(), "");
}

Status MetaStore::AddRange(uint64_t range_id, std::string &meta) {
    std::string key = kRangeMetaPrefix + std::to_string(range_id);

    rocksdb::Status ret = db_->Put(write_options_, key, meta);
    if (ret.ok()) {
        return Status::OK();
    }
    return Status(Status::kIOError, ret.ToString(), "put meta");
}

Status MetaStore::DelRange(uint64_t range_id) {
    std::string key = kRangeMetaPrefix + std::to_string(range_id);

    rocksdb::Status ret = db_->Delete(write_options_, key);
    if (ret.ok()) {
        return Status::OK();
    } else if (ret.IsNotFound()) {
        return Status(Status::kNotFound);
    } else {
        return Status(Status::kIOError, ret.ToString(), "");
    }
}

Status MetaStore::BatchAddRange(std::map<uint64_t, std::string> ranges) {
    rocksdb::WriteBatch bw;
    for (auto &it : ranges) {
        std::string key = kRangeMetaPrefix + std::to_string(it.first);
        bw.Put(key, it.second);
    }

    rocksdb::Status ret = db_->Write(write_options_, &bw);

    if (ret.ok()) {
        return Status::OK();
    }
    return Status(Status::kIOError, ret.ToString(), "");
}

Status MetaStore::SaveApplyIndex(uint64_t range_id, uint64_t apply_index) {
    std::string key = kRangeApplyPrefix + std::to_string(range_id);
    auto ret =
        db_->Put(rocksdb::WriteOptions(), key, std::to_string(apply_index));
    if (ret.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta save apply", ret.ToString());
    }
}

Status MetaStore::LoadApplyIndex(uint64_t range_id, uint64_t *apply_index) {
    std::string key = kRangeApplyPrefix + std::to_string(range_id);
    std::string value;
    auto ret = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (ret.ok()) {
        try {
            *apply_index = std::stoull(value);
        } catch (std::exception &e) {
            return Status(Status::kCorruption, "invalid applied",
                          EncodeToHexString(value));
        }
        return Status::OK();
    } else if (ret.IsNotFound()) {
        *apply_index = 0;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta load apply", ret.ToString());
    }
}

Status MetaStore::DeleteApplyIndex(uint64_t range_id) {
    std::string key = kRangeApplyPrefix + std::to_string(range_id);
    auto ret = db_->Delete(rocksdb::WriteOptions(), key);
    if (ret.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta delete apply", ret.ToString());
    }
}

}  // namespace storage
}  // namespace dataserver
}  // namespace sharkstore
