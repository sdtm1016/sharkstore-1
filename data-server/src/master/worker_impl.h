_Pragma("once");

#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#include "base/status.h"
#include "rpc_types.h"
#include "worker.h"

namespace sharkstore {
namespace dataserver {
namespace master {

class Client;
class TaskHandler;

class WorkerImpl : public Worker {
public:
    WorkerImpl(const std::vector<std::string> &ms_addrs, int node_hb_secs);
    ~WorkerImpl();

    WorkerImpl(const WorkerImpl &) = delete;
    WorkerImpl &operator=(const WorkerImpl &) = delete;

    Status GetNodeId(mspb::GetNodeIdRequest& req, uint64_t *node_id,
            bool *clearup) override;

    Status NodeLogin(uint64_t node_id) override;

    Status Start(TaskHandler *handler) override;
    void Stop() override;

    Status GetRaftAddress(uint64_t node_id, std::string *addr) override;

    void AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest &req) override;
    void AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest &req) override;
    void AsyncAskSplit(const mspb::AskSplitRequest &req) override;
    void AsyncReportSplit(const mspb::ReportSplitRequest &req) override;

    size_t GetRPCQueueSize() const;

private:
    struct AsyncRPCTask {
        AsyncCallType type;
        std::function<Status()> call_func;
    };

    // return true if pushed into queue
    // TODO: don't use raw pointer
    bool pushCall(AsyncRPCTask *task);
    void doCallRPC();
    void doNodeHeartbeat(TaskHandler *handler);

private:
    const size_t node_heartbeat_secs_ = 10;

    Client *client_ = nullptr;

    bool stopped_ = false;

    std::queue<AsyncRPCTask *> rpc_queue_;
    mutable std::mutex mu_;
    std::condition_variable cond_;

    std::thread send_rpc_thr_;
    std::thread node_hb_thr_;
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
