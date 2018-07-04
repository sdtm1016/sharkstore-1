_Pragma("once");

#include <rocksdb/db.h>
#include <rocksdb/utilities/blob_db/blob_db.h>
#include <mutex>

#include "iterator.h"
#include "metric.h"
#include "proto/gen/kvrpcpb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

// 行前缀长度: 1字节特殊标记+8字节table id
static const size_t kRowPrefixLength = 9;

class Store {
public:
    Store(const metapb::Range& meta, rocksdb::DB* db);
    ~Store();

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    Status Get(const std::string& key, std::string* value);
    Status Put(const std::string& key, const std::string& value);
    Status Delete(const std::string& key);

    Status Insert(const kvrpcpb::InsertRequest& req, uint64_t* affected);
    Status Select(const kvrpcpb::SelectRequest& req,
                  kvrpcpb::SelectResponse* resp);
    Status DeleteRows(const kvrpcpb::DeleteRequest& req, uint64_t* affected);
    Status Truncate();

    void SetEndKey(std::string end_key);

    const std::vector<metapb::Column>& GetPrimaryKeys() const {
        return primary_keys_;
    }

    uint64_t StatisSize(std::string& split_key, uint64_t split_size);
    uint64_t StatisSize(std::string& split_key, uint64_t split_size,
                        bool decode);  // fjf 2018-01-31

    void ResetMetric() { metric_.Reset(); }
    void CollectMetric(MetricStat* stat) { metric_.Collect(stat); }

public:
    Iterator* NewIterator(const ::kvrpcpb::Scope& scope);
    Iterator* NewIterator(std::string start = std::string(),
                          std::string limit = std::string());
    Status BatchDelete(const std::vector<std::string>& keys);
    bool KeyExists(const std::string& key);
    Status BatchSet(
        const std::vector<std::pair<std::string, std::string>>& keyValues);
    Status RangeDelete(const std::string& start, const std::string& limit);

    Status ApplySnapshot(const std::vector<std::string>& datas);

private:
    friend class RowFetcher;

    Status selectSimple(const kvrpcpb::SelectRequest& req,
                        kvrpcpb::SelectResponse* resp);
    Status selectAggre(const kvrpcpb::SelectRequest& req,
                       kvrpcpb::SelectResponse* resp);

    void addMetricRead(uint64_t keys, uint64_t bytes);
    void addMetricWrite(uint64_t keys, uint64_t bytes);

private:
    const uint64_t range_id_;
    const std::string start_key_;

    std::string end_key_;
    std::mutex key_lock_;

    rocksdb::DB* db_;
    rocksdb::WriteOptions write_options_;

    std::vector<metapb::Column> primary_keys_;

    Metric metric_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
