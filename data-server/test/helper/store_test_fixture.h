_Pragma("once");

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "base/status.h"
#include "storage/store.h"
#include "query_builder.h"
#include "range/split_policy.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace ::sharkstore::dataserver::range;
using namespace ::sharkstore::dataserver::storage;

class StoreTestFixture : public ::testing::Test {
public:
    explicit StoreTestFixture(std::unique_ptr<Table> t);

protected:
    void SetUp() override;
    void TearDown() override;

protected:
    // sql test methods:
    Status testSelect(const std::function<void(SelectRequestBuilder&)>& build_func,
                      const std::vector<std::vector<std::string>>& expected_rows);

    Status testInsert(const std::vector<std::vector<std::string>> &rows, uint64_t *insert_bytes= 0);

    Status testDelete(const std::function<void(DeleteRequestBuilder&)>& build_func,
                      uint64_t expected_affected);

protected:
    std::string encodeWatchKey(const std::vector<std::string>& keys);
    Status testParseWatchSplitKey(const std::vector<std::string>& keys);
    uint64_t statSizeUntil(const std::string& end);


protected:
    std::unique_ptr<Table> table_;
    metapb::Range meta_;
    dataserver::storage::Store* store_ = nullptr;

private:
    std::string tmp_dir_;
    rocksdb::DB* db_ = nullptr;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
