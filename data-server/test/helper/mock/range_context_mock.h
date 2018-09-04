_Pragma("once");

#include <atomic>
#include <mutex>

#include "range/context.h"
#include "raft/server.h"
#include "master/worker.h"
#include "watch/watch_server.h"

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;

namespace sharkstore {
namespace test {
namespace mock {

class RangeContextMock: public RangeContext {
public:
    Status Init();
    void Destroy();

    uint64_t GetNodeID() const override { return 1; }

    // 分裂策略
    SplitPolicy* GetSplitPolicy() override { return split_policy_.get(); }

    rocksdb::DB *DBInstance() override { return db_; }
    master::Worker* MasterClient() override { return master_worker_.get(); }
    raft::RaftServer* RaftServer() override { return raft_server_.get(); }
    storage::MetaStore* MetaStore() override { return meta_store_.get(); }
    common::SocketSession* SocketSession() override { return socket_session_.get(); }
    RangeStats* Statistics() override { return range_stats_.get(); }
    watch::WatchServer* WatchServer() override { return watch_server_.get(); }

    void SetFSUsagePercent(uint64_t value) { fs_usage_percent_ = value; }
    uint64_t GetFSUsagePercent() const override { return fs_usage_percent_.load(); }

    void ScheduleHeartbeat(uint64_t range_id, bool delay) override;
    void ScheduleCheckSize(uint64_t range_id) override;

    Status CreateRange(const metapb::Range& meta, uint64_t leader = 0,
            uint64_t index = 0, std::shared_ptr<Range> *result = nullptr);
    std::shared_ptr<Range> FindRange(uint64_t range_id) override;
    Status SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req, uint64_t raft_index) override;

private:
    std::string path_;
    rocksdb::DB *db_ = nullptr;
    std::unique_ptr<storage::MetaStore> meta_store_;
    std::unique_ptr<master::Worker> master_worker_;
    std::unique_ptr<raft::RaftServer> raft_server_;
    std::unique_ptr<common::SocketSession> socket_session_;
    std::unique_ptr<RangeStats> range_stats_;
    std::unique_ptr<SplitPolicy> split_policy_;
    std::unique_ptr<watch::WatchServer> watch_server_;

    std::atomic<uint64_t> fs_usage_percent_ = {0};

    std::map<uint64_t, std::shared_ptr<Range>> ranges_;
    std::mutex mu_;
};

}
}
}
