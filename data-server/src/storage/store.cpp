#include "store.h"
#include <common/ds_config.h>

#include "aggregate_calc.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_encoding.h"
#include "field_value.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/redispb.pb.h"
#include "row_fetcher.h"

namespace sharkstore {

namespace dataserver {
namespace storage {

static const size_t kDefaultMaxSelectLimit = 10000;

Store::Store(const metapb::Range& meta, rocksdb::DB* db) :
    table_id_(meta.table_id()) ,
    range_id_(meta.id()),
    start_key_(meta.start_key()),
    end_key_(meta.end_key()),
    db_(db) {
    assert(!start_key_.empty());
    assert(!end_key_.empty());
    assert(meta.primary_keys_size() > 0);
    for (int i = 0; i < meta.primary_keys_size(); ++i) {
        primary_keys_.push_back(meta.primary_keys(i));
    }

    write_options_.disableWAL = ds_config.rocksdb_config.disable_wal;
}

Store::~Store() {}

Status Store::Get(const std::string& key, std::string* value) {
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true), key, value);
    if (s.ok()) {
        addMetricRead(1, key.size() + value->size());
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else {
        return Status(Status::kIOError, "get", s.ToString());
    }
}

Status Store::Put(const std::string& key, const std::string& value) {
    rocksdb::Status s;
    if(ds_config.rocksdb_config.storage_type == 1 && ds_config.rocksdb_config.ttl > 0){
        auto *blobdb = static_cast<rocksdb::blob_db::BlobDB*>(db_);
        s = blobdb->PutWithTTL(write_options_,rocksdb::Slice(key),rocksdb::Slice(value),ds_config.rocksdb_config.ttl);
    }else{
        s = db_->Put(write_options_, key, value);
    }

    if (s.ok()) {
        addMetricWrite(1, key.size() + value.size());
        return Status::OK();
    }
    return Status(Status::kIOError, "put", s.ToString());
}

Status Store::Delete(const std::string& key) {
    rocksdb::Status s = db_->Delete(write_options_, key);
    if (s.ok()) {
        addMetricWrite(1, key.size());
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else {
        return Status(Status::kIOError, "delete", s.ToString());
    }
}

Status Store::Insert(const kvrpcpb::InsertRequest& req, uint64_t* affected) {
    if(ds_config.rocksdb_config.storage_type == 1 && ds_config.rocksdb_config.ttl > 0){
        auto *blobdb = static_cast<rocksdb::blob_db::BlobDB*>(db_);
        std::string value;
        rocksdb::Status s;
        bool check_dup = req.check_duplicate();
        *affected = 0;
        for (int i = 0; i < req.rows_size(); ++i) {
            const kvrpcpb::KeyValue& kv = req.rows(i);
            if (check_dup) {
                s = db_->Get(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true), kv.key(), &value);
                if (s.ok()) {
                    return Status(Status::kDuplicate);
                } else if (!s.IsNotFound()) {
                    return Status(Status::kIOError, "get", s.ToString());
                }
            }
            s = blobdb->PutWithTTL(write_options_,rocksdb::Slice(kv.key()),rocksdb::Slice(kv.value()),ds_config.rocksdb_config.ttl);
            if (!s.ok()) {
                return Status(Status::kIOError, "blobdb put", s.ToString());
            }else{
                addMetricWrite(*affected, kv.key().size()+kv.value().size());
                *affected = *affected + 1;
            }

        }

       return Status::OK();

    }

    uint64_t bytes_written = 0;
    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    std::string value;
    bool check_dup = req.check_duplicate();
    *affected = 0;
    for (int i = 0; i < req.rows_size(); ++i) {
        const kvrpcpb::KeyValue& kv = req.rows(i);
        if (check_dup) {
            s = db_->Get(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true), kv.key(), &value);
            if (s.ok()) {
                return Status(Status::kDuplicate);
            } else if (!s.IsNotFound()) {
                return Status(Status::kIOError, "get", s.ToString());
            }
        }
        s = batch.Put(kv.key(), kv.value());
        if (!s.ok()) {
            return Status(Status::kIOError, "batch put", s.ToString());
        }
        *affected = *affected + 1;
        bytes_written += (kv.key().size(), kv.value().size());
    }
    s = db_->Write(write_options_, &batch);
    if (!s.ok()) {
        return Status(Status::kIOError, "batch write", s.ToString());
    } else {
        addMetricWrite(*affected, bytes_written);
        return Status::OK();
    }
}

static void addRow(const kvrpcpb::SelectRequest& req,
                   kvrpcpb::SelectResponse* resp, const RowResult& r) {
    std::string buf;
    for (int i = 0; i < req.field_list_size(); i++) {
        const auto& f = req.field_list(i);
        if (f.has_column()) {
            FieldValue* v = r.GetField(f.column().id());
            EncodeFieldValue(&buf, v);
        }
    }
    auto row = resp->add_rows();
    row->set_key(r.Key());
    row->set_fields(buf);
}

Status Store::selectSimple(const kvrpcpb::SelectRequest& req,
                           kvrpcpb::SelectResponse* resp) {
    RowFetcher f(*this, req);
    Status s;
    std::unique_ptr<RowResult> r(new RowResult);
    bool over = false;
    uint64_t count = 0;
    uint64_t all = 0;
    uint64_t limit = req.has_limit() ? req.limit().count() : kDefaultMaxSelectLimit;
    uint64_t offset = req.has_limit() ? req.limit().offset() : 0;
    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            ++all;
            if (all > offset) {
                addRow(req, resp, *r);
                if (++count >= limit) break;
            }
        }
    }
    resp->set_offset(all);
    return s;
}

Status Store::selectAggre(const kvrpcpb::SelectRequest& req,
                          kvrpcpb::SelectResponse* resp) {
    // 暂时不支持带group by的聚合函数
    if (req.group_bys_size() > 0) {
        return Status(Status::kNotSupported, "select",
                      "aggregateion with group by clause");
    }

    std::vector<std::unique_ptr<AggreCalculator>> aggre_cals;
    aggre_cals.reserve(req.field_list_size());
    for (int i = 0; i < req.field_list_size(); ++i) {
        const auto& field = req.field_list(i);
        assert(field.typ() == kvrpcpb::SelectField_Type_AggreFunction);
        // TODO:
        auto cal = AggreCalculator::New(
            field.aggre_func(), field.has_column() ? &field.column() : nullptr);
        if (cal == nullptr) {
            return Status(
                Status::kNotSupported, "select",
                std::string("aggregate funtion: ") + field.aggre_func());
        } else {
            aggre_cals.push_back(std::move(cal));
        }
    }

    RowFetcher f(*this, req);
    Status s;
    std::unique_ptr<RowResult> r(new RowResult);
    bool over = false;
    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            for (size_t i = 0; i < aggre_cals.size(); ++i) {
                const auto& field = req.field_list(i);
                if (field.has_column()) {
                    aggre_cals[i]->Add(r->GetField(field.column().id()));
                } else {
                    aggre_cals[i]->Add(nullptr);
                }
            }
        }
    }
    if (s.ok()) {
        std::string buf;
        auto row = resp->add_rows();
        for (auto& cal : aggre_cals) {
            auto f = cal->Result();
            EncodeFieldValue(&buf, f.get());
            row->add_aggred_counts(cal->Count());
        }
        row->set_fields(buf);
    }
    return s;
}

Status Store::Select(const kvrpcpb::SelectRequest& req,
                     kvrpcpb::SelectResponse* resp) {
    if (req.field_list_size() == 0) {
        return Status(Status::kNotSupported, "select",
                      "invalid select field list size");
    }

    bool has_aggre = false, has_column = false;
    for (int i = 0; i < req.field_list_size(); ++i) {
        auto type = req.field_list(i).typ();
        switch (type) {
            case kvrpcpb::SelectField_Type_Column:
                has_column = true;
                break;
            case kvrpcpb::SelectField_Type_AggreFunction:
                has_aggre = true;
                break;
            default:
                return Status(Status::kInvalidArgument, "select",
                              std::string("unknown select field type: ") +
                                  kvrpcpb::SelectField_Type_Name(type));
        }
    }
    // 既有聚合函数又有普通的列，暂时不支持
    if (has_aggre && has_column) {
        return Status(Status::kNotSupported, "select",
                      "mixture of aggregate and column select field");
    } else if (has_column) {
        return selectSimple(req, resp);
    } else {
        return selectAggre(req, resp);
    }
}

Status Store::DeleteRows(const kvrpcpb::DeleteRequest& req,
                         uint64_t* affected) {
    RowFetcher f(*this, req);
    Status s;
    std::unique_ptr<RowResult> r(new RowResult);
    bool over = false;
    rocksdb::WriteBatch batch;
    uint64_t bytes_written = 0;

    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            assert(!r->Key().empty());
            batch.Delete(r->Key());
            ++(*affected);
            bytes_written += r->Key().size();
        }
    }

    if (s.ok()) {
        auto rs = db_->Write(write_options_, &batch);
        if (!rs.ok()) {
            s = Status(Status::kIOError, "delete batch write", rs.ToString());
        } else {
            addMetricWrite(*affected, bytes_written);
        }
    }

    return s;
}

Status Store::Truncate() {
    rocksdb::WriteOptions op;

    std::unique_lock<std::mutex> lock(key_lock_);
    auto family = db_->DefaultColumnFamily();

    assert(!start_key_.empty());
    assert(!end_key_.empty());
    assert(start_key_ < end_key_);

    auto s = db_->DeleteRange(op, family, start_key_, end_key_);
    if (!s.ok()) {
        return Status(Status::kIOError, "delete range", s.ToString());
    }

    return Status::OK();
};

void Store::SetEndKey(std::string end_key) {
    std::unique_lock<std::mutex> lock(key_lock_);
    assert(start_key_ < end_key);
    end_key_ = std::move(end_key);
}

std::string Store::GetEndKey() const {
    std::unique_lock<std::mutex> lock(key_lock_);
    return end_key_;
}

Iterator* Store::NewIterator(const kvrpcpb::Scope& scope) {
    auto it = db_->NewIterator(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true));
    std::string start = scope.start();
    std::string limit = scope.limit();
    if (start.empty() || start < start_key_) {
        start = start_key_;
    }

    {
        std::unique_lock<std::mutex> lock(key_lock_);
        if (limit.empty() || limit > end_key_) {
            limit = end_key_;
        }
    }
    return new Iterator(it, start, limit);
}

Iterator* Store::NewIterator(std::string start, std::string limit) {
    auto it = db_->NewIterator(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true));
    if (start.empty() || start < start_key_) {
        start = start_key_;
    }

    {
        std::unique_lock<std::mutex> lock(key_lock_);
        if (limit.empty() || limit > end_key_) {
            limit = end_key_;
        }
    }
    return new Iterator(it, start, limit);
}

Status Store::BatchDelete(const std::vector<std::string>& keys) {
    if (keys.empty()) return Status::OK();

    uint64_t keys_written = 0;
    uint64_t bytes_written = 0;

    rocksdb::WriteBatch batch;
    for (auto& key : keys) {
        batch.Delete(key);
        ++keys_written;
        bytes_written += key.size();
    }
    auto ret = db_->Write(write_options_, &batch);
    if (ret.ok()) {
        addMetricWrite(keys_written, bytes_written);
        return Status::OK();
    } else {
        return Status(Status::kIOError, "BatchDelete", ret.ToString());
    }
}

bool Store::KeyExists(const std::string& key) {
    rocksdb::PinnableSlice value;
    auto ret = db_->Get(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true), db_->DefaultColumnFamily(), key,
                        &value);
    addMetricRead(1, key.size() + value.size());
    return ret.ok();
}

Status Store::BatchSet(
    const std::vector<std::pair<std::string, std::string>>& keyValues) {
    if (keyValues.empty()) return Status::OK();

    uint64_t keys_written = 0;
    uint64_t bytes_written = 0;

    rocksdb::WriteBatch batch;
    for (auto& kv : keyValues) {
        batch.Put(kv.first, kv.second);
        ++keys_written;
        bytes_written += (kv.first.size() + kv.second.size());
    }
    auto ret = db_->Write(write_options_, &batch);
    if (ret.ok()) {
        addMetricWrite(keys_written, bytes_written);
        return Status::OK();
    } else {
        return Status(Status::kIOError, "BatchSet", ret.ToString());
    }
}

Status Store::RangeDelete(const std::string& start, const std::string& limit) {
    auto ret = db_->DeleteRange(write_options_, db_->DefaultColumnFamily(),
                                start, limit);
    return Status(ret.ok() ? Status::OK() : Status(Status::kUnknown));
}

Status Store::ApplySnapshot(const std::vector<std::string>& datas) {
    rocksdb::WriteBatch batch;
    for (const auto& data : datas) {
        raft_cmdpb::SnapshotKVPair p;
        if (!p.ParseFromString(data)) {
            return Status(Status::kCorruption, "apply snapshot data",
                          "deserilize return false");
        } else {
            batch.Put(p.key(), p.value());
        }
    }
    auto ret = db_->Write(write_options_, &batch);
    if (!ret.ok()) {
        return Status(Status::kIOError, "snap batch write", ret.ToString());
    } else {
        return Status::OK();
    }
}

void Store::addMetricRead(uint64_t keys, uint64_t bytes) {
    metric_.AddRead(keys, bytes);
    g_metric.AddRead(keys, bytes);
}

void Store::addMetricWrite(uint64_t keys, uint64_t bytes) {
    metric_.AddWrite(keys, bytes);
    g_metric.AddWrite(keys, bytes);
}

Status Store::parseSplitKey(const std::string& key, range::SplitKeyMode mode, std::string *split_key) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient key size", EncodeToHex(key));
    }

    switch (mode) {
        case range::SplitKeyMode::kNormal:
            *split_key = key;
            return Status::OK();
        case range::SplitKeyMode::kRedis: {
            size_t offset = kRowPrefixLength;
            // decode ns
            if (!DecodeVarintAscending(key, offset, nullptr)) {
                return Status(Status::kCorruption, "decode redis value ns", EncodeToHex(key));
            }
            // decode real key
            std::string realKey;
            if (!DecodeBytesAscending(key, offset, &realKey)) {
                return Status(Status::kCorruption, "decode redis value real key", EncodeToHex(key));
            }
            assert(offset <= key.size());
            split_key->assign(key.c_str(), offset);
            return Status::OK();
        }
        case range::SplitKeyMode::kLockWatch:{
            size_t offset = kRowPrefixLength;
            std::string watch_key;
            if (!DecodeBytesAscending(key, offset, &watch_key)) {
                return Status(Status::kCorruption, "decode watch key", EncodeToHex(key));
            }
            assert(offset <= key.size());
            split_key->assign(key.c_str(), offset);
            return Status::OK();
        }
        default:
            return Status(Status::kNotSupported, "split key mode",
                    std::to_string(static_cast<int>(mode)));
    }
}

Status Store::StatSize(uint64_t split_size, range::SplitKeyMode mode,
                  uint64_t *real_size, std::string *split_key) {
    uint64_t total_size = 0;

    // The number of the same characters is greater than
    // the length of start_key_ and more than 5,
    // then the length of the split_key is
    // start_key_.length() + 5
    auto max_len = start_key_.length() + 5;

    std::unique_ptr<Iterator> it(NewIterator());
    std::string middle_key;
    std::string first_key;

    while (it->Valid()) {
        if (mode != range::SplitKeyMode::kNormal && first_key.empty()) {
            auto s = parseSplitKey(it->key(), mode, &first_key);
            if (!s.ok()) return s;
            assert(!first_key.empty());
        }

        total_size += it->key_size();
        total_size += it->value_size();

        if (total_size >= split_size) {
            middle_key = it->key();
            it->Next();
            break;
        }

        it->Next();
    }

    if (!it->Valid()) {
        if (!it->status().ok()) {
            return it->status();
        } else {
            return Status(Status::kUnexpected, "no more data", std::to_string(total_size));
        }
    }

    // 特殊模式（非Normal）split key会是原始key的前缀，
    // 如果first key跟split key相等， 则需要查找下一个前缀key作为split key,
    // 以防止 [start_key, split_key) 区间内的数据为空
    bool find_next = false;
    if (mode == range::SplitKeyMode::kNormal) {
        *split_key = SliceSeparate(it->key(), middle_key, max_len);
    } else {
        auto s = parseSplitKey(middle_key, mode, split_key);
        if (!s.ok()) {
            return s;
        }
        // 相等，需要找下一个
        find_next = (first_key == *split_key);
    }

    // 遍历剩下一的一半
    while (it->Valid()) {
        total_size += it->key_size();
        total_size += it->value_size();

        if (find_next) {
            assert(mode != range::SplitKeyMode::kNormal);
            auto s = parseSplitKey(it->key(), mode, split_key);
            if (!s.ok()) return s;
            if (*split_key != first_key) { // 遇到不一样的key
                find_next = false;
            }
        }

        it->Next();
    }
    if (!it->status().ok()) {
        return it->status();
    }

    if (find_next) {
        return Status(Status::kNotFound, "next split key", EncodeToHex(*split_key));
    }

    *real_size = total_size;
    return Status::OK();
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
