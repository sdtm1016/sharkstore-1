_Pragma("once");

#include <gtest/gtest.h>
#include "range/range.h"
#include "range/context.h"
#include "storage/meta_store.h"

#include "mock/range_context_mock.h"

#include "table.h"

namespace sharkstore {
namespace test {
namespace helper {

// for test only
using namespace kvrpcpb;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;

// 构造一个有三个raft成员的range(NodeID: 1, 2, 3)
// 本地节点NodeID为1
class RangeTestFixture : public ::testing::Test {
protected:
    void SetUp() override;
    void TearDown() override;

protected:
    uint64_t GetNodeID() const { return range_->node_id_; }
    uint64_t GetRangeID() const { return range_->id_; }
    uint64_t GetSplitRangeID() const { return range_->split_range_id_; }

    // version=0表示传range当前version
    void MakeHeader(RequestHeader *header, uint64_t version = 0);
    void SetLeader(uint64_t leader);

    Status Split();

    Status TestInsert(DsInsertRequest &req, DsInsertResponse *resp);
    Status TestSelect(DsSelectRequest& req, DsSelectResponse* resp);
    Status TestDelete(DsDeleteRequest& req, DsDeleteResponse* resp);

    // for debug
    void SetLogLevel(char *level);

private:
    Status getResult(google::protobuf::Message *resp);

protected:
    std::unique_ptr<mock::RangeContextMock> context_;
    std::unique_ptr<Table> table_;
    std::shared_ptr<dataserver::range::Range> range_;
    uint64_t term_ = 0;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
