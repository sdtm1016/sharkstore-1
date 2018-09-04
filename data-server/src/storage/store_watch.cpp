#include "store.h"

#include "base/util.h"
#include "common/ds_encoding.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

std::string Store::encodeWatchKey(const watchpb::WatchKeyValue& kv) const {
    std::string buf;
    buf.push_back(kStoreKVPrefixByte);
    EncodeUint64Ascending(&buf, table_id_);
    for (const auto& k : kv.key()) {
        EncodeBytesAscending(&buf, k.c_str(), k.size());
    }
    return buf;
}

std::string Store::encodeWatchValue(const watchpb::WatchKeyValue& kv, int64_t version) const {
    static const std::string kExtendValue = "reserved";
    std::string buf;
    EncodeIntValue(&buf, 2, version);
    EncodeBytesValue(&buf, 3, kv.value().c_str(), kv.value().size());
    EncodeBytesValue(&buf, 4, kExtendValue.c_str(), kExtendValue.size());
    return buf;
}

bool Store::decodeWatchKey(const std::string& key, watchpb::WatchKeyValue *kv) const {
    for (size_t offset = kRowPrefixLength; offset < key.length(); ) {
        if (!DecodeBytesAscending(key, offset, kv->add_key())) {
            return false;
        }
    }
    return true;
}

bool Store::decodeWatchValue(const std::string& value, watchpb::WatchKeyValue *kv) const {
    size_t offset = 0;
    int64_t version = 0;
    if (!DecodeIntValue(value, offset, &version)) return false;
    kv->set_version(version);
    if (!DecodeBytesValue(value, offset, kv->mutable_value())) return false;
    return DecodeBytesValue(value, offset, kv->mutable_ext());
}


Status Store::WatchPut(const watchpb::KvWatchPutRequest& req, int64_t version) {
    if (req.kv().key_size() == 0) {
        return Status(Status::kInvalidArgument, "insufficient keys size",
                      std::to_string(req.kv().key_size()));
    }
    auto key = encodeWatchKey(req.kv());
    auto value = encodeWatchValue(req.kv(), version);
    return this->Put(key, value);
}

Status Store::WatchDelete(const watchpb::KvWatchDeleteRequest& req) {
    if (req.kv().key_size() == 0) {
        return Status(Status::kInvalidArgument, "insufficient keys size",
                std::to_string(req.kv().key_size()));
    }

    if (!req.prefix()) {
        return this->Delete(encodeWatchKey(req.kv()));
    } else {
        // TODO:
        return Status(Status::kNotSupported);
    }
}

Status Store::WatchGet(const watchpb::DsKvWatchGetMultiRequest& req,
                       watchpb::DsKvWatchGetMultiResponse *resp) {
    // get key
    if (!req.prefix()) {
        auto key = encodeWatchKey(req.kv());
        std::string value;
        auto s = this->Get(key, &value);
        if (!s.ok()) return s;

        auto resp_kv = resp->add_kvs();
        if (!decodeWatchValue(value, resp_kv)) {
            return Status(Status::kCorruption, "decode watch value", EncodeToHex(value));
        }

        for (const auto& k : req.kv().key()) {
            resp_kv->add_key(k);
        }
        return Status::OK();
    }

    // get prefix
    auto start = encodeWatchKey(req.kv());
    auto limit = NextComparable(start);
    assert(!limit.empty());
    std::unique_ptr<Iterator> iter(NewIterator(start ,limit));
    while (iter->Valid()) {
        auto resp_kv = resp->add_kvs();
        if (!decodeWatchKey(iter->key(), resp_kv)) {
            return Status(Status::kCorruption, "decode watch key", EncodeToHex(iter->key()));
        }
        if (!decodeWatchValue(iter->value(), resp_kv)) {
            return Status(Status::kCorruption, "decode watch value", EncodeToHex(iter->value()));
        }
        iter->Next();
    }
    return iter->status();
}

Status Store::WatchScan() {
    return Status(Status::kNotSupported);
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
