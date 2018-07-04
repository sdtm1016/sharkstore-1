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

//声明一个KEY
static std::string GetRealKey(std::string& key) {
    if (key.size() <= kRowPrefixLength) return "";
    size_t pos = kRowPrefixLength;
    // skip ns
    if (!DecodeVarintAscending(key, pos, nullptr)) return "";
    std::string realKey;
    if (!DecodeBytesAscending(key, pos, &realKey)) return "";
    int64_t type = 0;
    if (!DecodeVarintAscending(key, pos, &type)) return "";

    return realKey;
}

Store::Store(const metapb::Range& meta, rocksdb::DB* db)
    : range_id_(meta.id()),
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
    uint64_t limit =
        req.has_limit() ? req.limit().count() : kDefaultMaxSelectLimit;
    uint64_t offset = req.has_limit() ? req.limit().offset() : 0;
    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            ++all;
            if (count > limit) {
                break;
            }
            if (all > offset) {
                addRow(req, resp, *r);
                ++count;
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

uint64_t Store::StatisSize(std::string& split_key, uint64_t split_size) {
    uint64_t len = 0;

    // The number of the same characters is greater than
    // the length of start_key_ and more than 5,
    // then the length of the split_key is
    // start_key_.length() + 5
    auto max_len = start_key_.length() + 5;

    std::unique_ptr<Iterator> it(NewIterator());

    while (it->Valid()) {
        len += it->key_size();
        len += it->value_size();

        if (len > split_size) {
            split_key = std::move(it->key());
            it->Next();
            break;
        }

        it->Next();
    }

    if (it->Valid() && !split_key.empty()) {
        split_key = SliceSeparate(it->key(), split_key, max_len);
    } else {
        split_key.clear();
    }

    while (it->Valid()) {
        len += it->key_size();
        len += it->value_size();

        it->Next();
    }

    return len;
}
uint64_t Store::StatisSize(std::string& split_key, uint64_t split_size,
                           bool decode) {
    uint64_t len = 0;
    std::string cur_real_key = "";  //解析后的实际KEY
    std::string pre_real_key = "";
    std::string curkey = "";  //未解析得到的KEY

    auto max_len = start_key_.length() + 5;

    std::unique_ptr<Iterator> it(NewIterator());

    while (it->Valid()) {
        len += it->key_size();
        len += it->value_size();

        //由编码后的KEY，解析当前实际KEY
        curkey = std::move(it->key());
        pre_real_key = std::move(cur_real_key);
        cur_real_key = std::move(GetRealKey(curkey));

        if (len > split_size) {
            split_key = std::move(curkey);
            if ((!pre_real_key.empty()) && (cur_real_key != pre_real_key)) {
                it->Next();
                break;
            }
        }

        it->Next();
    }

    if (!it->Valid()) {
        split_key.clear();
    }

    while (it->Valid()) {
        len += it->key_size();
        len += it->value_size();

        it->Next();
    }

    return len;
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

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
