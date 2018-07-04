_Pragma("once");

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "base/status.h"
#include "connection.h"
#include "task_handler.h"

namespace sharkstore {
namespace dataserver {
namespace master {

struct AsyncCallResult;

// Client 管理同MasterServer之间的RPC请求
// 所有请求如果当前没有leader，会先获取一次leader，如果获取失败则本次请求失败
class Client final {
public:
    explicit Client(const std::vector<std::string>& ms_addrs);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // start async grpc cq thread
    // and use the handler to handle responses from master server
    void Start(TaskHandler* handler);

    // 同步调用方法
    Status GetNodeID(const mspb::GetNodeIdRequest& req, uint64_t* node_id,
                     bool* clearup);

    Status NodeLogin(uint64_t node_id);

    Status GetNodeAddress(uint64_t node_id, std::string* server_addr,
                          std::string* raft_addr, std::string* http_addr);

    // 异步调用方法
    Status AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest& req);
    Status AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest& req);
    Status AsyncAskSplit(const mspb::AskSplitRequest& req);
    Status AsyncReportSplit(const mspb::ReportSplitRequest& req);

private:
    using ConnPtr = std::shared_ptr<Connection>;

    // 设置leader地址
    void set_leader(const std::string& leader);
    // 获取leader地址
    std::string leader() const;
    // 没有leader地址的情况下，挑选一个master server 地址
    std::string candidate();
    // 添加一个master server地址 unlocked
    void addAddr(const std::string& addr);

    // 根据GetMSSResponse回应更新leader
    void updateLeader(const std::string& from,
                      const mspb::GetMSLeaderResponse& resp);
    // 挑一个master地址，询问leader
    void askLeader();
    void asyncAskLeader();

    ConnPtr getConnection(const std::string& to);
    ConnPtr getLeaderConn();

    void recvResponse(TaskHandler* handler);
    void dispatchResponse(TaskHandler* handler, const std::string& from,
                          AsyncCallResult* resp);

    // 检查rpc状态马，如果连续多次都是网络错误，我们可能连到了一个假的leader
    // 就发起一次AsyncAskLeader请求
    void checkStatus(const grpc::Status& s);

    // return true if we has error in the header
    // and maybe need to update leader according to this error
    bool checkResponseError(const std::string& from,
                            const mspb::ResponseHeader& header);

private:
    using ConnMap = std::map<std::string, ConnPtr>;
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    std::vector<std::string> ms_addrs_;
    std::string leader_;
    mutable std::mutex addr_mu_;
    size_t magic_counter_ = 0;

    std::atomic<uint64_t> network_fail_count_{0};
    TimePoint last_async_ask_;

    ConnMap connections_;
    std::mutex conn_mu_;
    grpc::CompletionQueue cq_;
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
