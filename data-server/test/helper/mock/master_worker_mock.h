_Pragma("once");

#include "master/worker.h"

namespace sharkstore {
namespace test {
namespace mock {

using namespace sharkstore::dataserver;

class MasterWorkerMock : public master::Worker {
public:
    Status GetNodeId(mspb::GetNodeIdRequest& req,
                     uint64_t *node_id, bool *clearup) override;

    Status NodeLogin(uint64_t node_id) override;

    Status Start(master::TaskHandler *handler) override;
    void Stop() override;

    Status GetRaftAddress(uint64_t node_id, std::string *addr) override;

    void AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest &req) override;
    void AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest &req) override;
    void AsyncAskSplit(const mspb::AskSplitRequest &req) override;
    void AsyncReportSplit(const mspb::ReportSplitRequest &req) override;
};

}
}
}
