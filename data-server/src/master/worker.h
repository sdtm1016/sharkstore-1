_Pragma("once");

#include "base/status.h"
#include "proto/gen/mspb.pb.h"
#include "task_handler.h"

namespace sharkstore {
namespace dataserver {
namespace master {

class Worker {
public:
    virtual ~Worker() = default;

    virtual Status GetNodeId(mspb::GetNodeIdRequest& req,
            uint64_t *node_id, bool *clearup) = 0;

    virtual Status NodeLogin(uint64_t node_id) = 0;

    virtual Status Start(TaskHandler *handler) = 0;
    virtual void Stop() = 0;

    virtual Status GetRaftAddress(uint64_t node_id, std::string *addr) = 0;

    virtual void AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest &req) = 0;
    virtual void AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest &req) = 0;
    virtual void AsyncAskSplit(const mspb::AskSplitRequest &req) = 0;
    virtual void AsyncReportSplit(const mspb::ReportSplitRequest &req) = 0;
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
