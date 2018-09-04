_Pragma("once");

#include <rocksdb/db.h>
#include "proto/gen/kvrpcpb.pb.h"
#include "row_decoder.h"
#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RowFetcher {
public:
    RowFetcher(Store& s, const kvrpcpb::SelectRequest& req);
    RowFetcher(Store& s, const kvrpcpb::DeleteRequest& req);

    ~RowFetcher();

    RowFetcher(const RowFetcher&) = delete;
    RowFetcher& operator=(const RowFetcher&) = delete;

    Status Next(RowResult* result, bool* over);

private:
    void init(const std::string& key, const ::kvrpcpb::Scope& scope);
    Status nextOneKey(RowResult* result, bool* over);
    Status nextScope(RowResult* result, bool* over);

private:
    Store& store_;
    RowDecoder decoder_;

    std::string key_;
    Iterator* iter_ = nullptr;
    Status last_status_;
    bool matched_ = false;
    size_t iter_count_ = 0;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
