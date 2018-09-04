#include "meta_store.h"

#include <errno.h>
#include <rocksdb/write_batch.h>
#include <memory>

#include "base/util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

MetaStore::MetaStore(const std::string &path) : path_(path) {
    write_options_.sync = true;
}

MetaStore::~MetaStore() { delete db_; }

Status MetaStore::Open(bool read_only) {
    int ret = MakeDirAll(path_, 0755);
    if (ret != 0) {
        return Status(Status::kIOError, "create meta store directory",
                      strErrno(errno));
    }

    rocksdb::Options ops;
    ops.create_if_missing = true;
    rocksdb::Status rs;
    if (read_only) {
        rs = rocksdb::DB::OpenForReadOnly(ops, path_, &db_);
    } else {
        rs = rocksdb::DB::Open(ops, path_, &db_);
    }
    if (!rs.ok()) {
        return Status(Status::kIOError, "open meta store db", rs.ToString());
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
            return Status(Status::kCorruption, "invalid node_id", EncodeToHex(value));
        }
        return Status::OK();
    } else if (ret.IsNotFound()) {
        *node_id = 0;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta load node", ret.ToString());
    }
}

Status MetaStore::SaveVersionID(const uint64_t &range_id, int64_t ver_id) {
    std::string keyRangeVer = kRangeVersionPrefix + std::to_string(range_id);

    auto ret = db_->Put(write_options_, keyRangeVer, std::to_string(ver_id));
    if (ret.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, ret.ToString(), "meta save version");
    }
}

Status MetaStore::GetVersionID(const uint64_t &range_id, int64_t *ver_id) {
    std::string value;
    std::string keyRangeVer = kRangeVersionPrefix + std::to_string(range_id);

    auto ret = db_->Get(rocksdb::ReadOptions(), keyRangeVer, &value);
    if (ret.ok()) {
        try {
            *ver_id = std::stoull(value);
        } catch (std::exception &e) {
            return Status(Status::kCorruption, "invalid version_id", EncodeToHex(value));
        }
        return Status::OK();
    } else if (ret.IsNotFound()) {
        *ver_id = 0;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "meta load version", ret.ToString());
    }
}

Status MetaStore::GetAllRange(std::vector<metapb::Range>* range_metas) {
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    it->Seek(kRangeMetaPrefix);
    while (it->Valid() && it->key().starts_with(kRangeMetaPrefix)) {
        metapb::Range rng;
        if (!rng.ParseFromArray(it->value().data(), static_cast<int>(it->value().size()))) {
            return Status(Status::kCorruption, "parse", it->value().ToString(true));
        }
        range_metas->push_back(std::move(rng));
        it->Next();
    }
    if (!it->status().ok()) {
        return Status(Status::kIOError, "iterator", it->status().ToString());
    }
    return Status::OK();
}

Status MetaStore::GetRange(uint64_t range_id, metapb::Range* meta) {
    std::string key = kRangeMetaPrefix + std::to_string(range_id);

    std::string value;
    auto s = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (s.IsNotFound()) {
        return Status(Status::kNotFound, "get range", "");
    } else if (!s.ok()) {
        return Status(Status::kCorruption, "get range", s.ToString());
    }

    if (!meta->ParseFromString(value)) {
        return Status(Status::kCorruption, "parse", EncodeToHex(value));
    }
    return Status::OK();
}

Status MetaStore::AddRange(const metapb::Range& meta) {
    std::string key = kRangeMetaPrefix + std::to_string(meta.id());
    // serialize
    std::string value;
    if (!meta.SerializeToString(&value)) {
        return Status(Status::kCorruption, "serialize", meta.DebugString());
    }
    // put into db
    rocksdb::Status ret = db_->Put(write_options_, key, value);
    if (!ret.ok()) {
        return Status(Status::kIOError, "put", ret.ToString());
    }
    return Status::OK();
}

Status MetaStore::BatchAddRange(const std::vector<metapb::Range>& range_metas) {
    rocksdb::WriteBatch batch;
    for (const auto& meta: range_metas) {
        std::string value;
        if (!meta.SerializeToString(&value)) {
            return Status(Status::kCorruption, "serialize", meta.DebugString());
        }
        std::string key = kRangeMetaPrefix + std::to_string(meta.id());
        batch.Put(key, value);
    }

    rocksdb::WriteOptions wops;
    wops.sync = true;
    rocksdb::Status ret = db_->Write(wops, &batch);
    if (!ret.ok()) {
        return Status(Status::kIOError, "batch write", ret.ToString());
    }
    return Status::OK();
}

Status MetaStore::DelRange(uint64_t range_id) {
    std::string key = kRangeMetaPrefix + std::to_string(range_id);
    rocksdb::Status ret = db_->Delete(write_options_, key);
    if (ret.ok()) {
        return Status::OK();
    } else if (ret.IsNotFound()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "delete range meta", ret.ToString());
    }
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
            return Status(Status::kCorruption, "invalid applied", EncodeToHex(value));
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
