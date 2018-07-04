_Pragma("once");

#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#include "base/status.h"
#include "rpc_types.h"

namespace sharkstore {
namespace dataserver {
namespace master {

class Client;
class TaskHandler;

class Worker final {
public:
    Worker(const std::vector<std::string> &ms_addrs, int node_hb_secs);
    ~Worker();

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;

    Status GetNodeId(uint16_t raft_port, uint16_t srv_port, uint16_t http_port,
                     const std::string &version, uint64_t *node_id,
                     bool *clearup);

    Status NodeLogin(uint64_t node_id);

    Status Start(TaskHandler *handler);
    void Stop();

    Status GetRaftAddress(uint64_t node_id, std::string *addr);
    Status GetServerAddress(uint64_t node_id, std::string *addr);

    void AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest &req);
    void AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest &req);
    void AsyncAskSplit(const mspb::AskSplitRequest &req);
    void AsyncReportSplit(const mspb::ReportSplitRequest &req);

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
