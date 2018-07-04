#include "worker.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "base/util.h"
#include "client.h"
#include "common/ds_config.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace master {

static const int kRcpRetryTimeoutSecs = 3;

Worker::Worker(const std::vector<std::string> &ms_addrs, int node_hb_secs)
    : node_heartbeat_secs_(node_hb_secs), client_(new Client(ms_addrs)) {}

Worker::~Worker() {
    this->Stop();
    {
        std::unique_lock<std::mutex> lock(mu_);
        while (!rpc_queue_.empty()) {
            delete rpc_queue_.front();
            rpc_queue_.pop();
        }
    }
    delete client_;
}

Status Worker::GetNodeId(uint16_t raft_port, uint16_t srv_port,
                         uint16_t http_port, const std::string &version,
                         uint64_t *node_id, bool *clearup) {
    mspb::GetNodeIdRequest req;
    req.set_raft_port(raft_port);
    req.set_server_port(srv_port);
    req.set_http_port(http_port);
    req.set_version(version);

    while (true) {
        auto s = client_->GetNodeID(req, node_id, clearup);
        if (s.ok()) {
            FLOG_INFO(
                "[Master] GetNodeId successfully. node_id=%lu, clearup=%d",
                *node_id, *clearup);
            return s;
        } else {
            FLOG_ERROR("[Master] GetNodeId failed(%s).", s.ToString().c_str());

            // 等待被stop或者重试sleep时间到
            std::unique_lock<std::mutex> lock(mu_);
            if (cond_.wait_for(lock, std::chrono::seconds(kRcpRetryTimeoutSecs),
                               [this]() { return stopped_; })) {
                return Status(Status::kShutdownInProgress);
            }
        }
    }
}

Status Worker::NodeLogin(uint64_t node_id) {
    while (true) {
        auto s = client_->NodeLogin(node_id);
        if (s.ok()) {
            FLOG_INFO("[Master] NodeLogin successfully. ");
            return s;
        } else {
            FLOG_ERROR("[Master] NodeLogin failed(%s).", s.ToString().c_str());

            // 等待被stop或者重试sleep时间到
            std::unique_lock<std::mutex> lock(mu_);
            if (cond_.wait_for(lock, std::chrono::seconds(kRcpRetryTimeoutSecs),
                               [this]() { return stopped_; })) {
                return Status(Status::kShutdownInProgress);
            }
        }
    }
}

Status Worker::Start(TaskHandler *handler) {
    FLOG_INFO("[Master] Worker Start begin ...");

    assert(handler != nullptr);

    client_->Start(handler);

    send_rpc_thr_ = std::thread(&Worker::doCallRPC, this);
    auto handle = send_rpc_thr_.native_handle();
    AnnotateThread(handle, "send_rpc");

    node_hb_thr_ = std::thread(&Worker::doNodeHeartbeat, this, handler);
    handle = node_hb_thr_.native_handle();
    AnnotateThread(handle, "node_hb");

    FLOG_INFO("[Master] Worker Start end ...");
    return Status::OK();
}

void Worker::Stop() {
    FLOG_INFO("Master Worker Stop begin ...");

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) return;
        stopped_ = true;
    }
    cond_.notify_all();

    if (node_hb_thr_.joinable()) {
        node_hb_thr_.join();
    }
    if (send_rpc_thr_.joinable()) {
        send_rpc_thr_.join();
    }

    FLOG_INFO("Master Worker Stop end ...");
}

void Worker::AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest &req) {
    auto task = new AsyncRPCTask;
    task->type = AsyncCallType::kNodeHeartbeat;
    task->call_func = std::bind(&Client::AsyncNodeHeartbeat, client_, req);
    if (!pushCall(task)) {
        delete task;
    }
}

void Worker::AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest &req) {
    auto task = new AsyncRPCTask;
    task->type = AsyncCallType::kRangeHeartbeat;
    task->call_func = std::bind(&Client::AsyncRangeHeartbeat, client_, req);
    if (!pushCall(task)) {
        delete task;
    }
}

void Worker::AsyncAskSplit(const mspb::AskSplitRequest &req) {
    auto task = new AsyncRPCTask;
    task->type = AsyncCallType::kAskSplit;
    task->call_func = std::bind(&Client::AsyncAskSplit, client_, req);
    if (!pushCall(task)) {
        delete task;
    }
}

void Worker::AsyncReportSplit(const mspb::ReportSplitRequest &req) {
    auto task = new AsyncRPCTask;
    task->type = AsyncCallType::kReportSplit;
    task->call_func = std::bind(&Client::AsyncReportSplit, client_, req);
    if (!pushCall(task)) {
        delete task;
    }
}

size_t Worker::GetRPCQueueSize() const {
    std::lock_guard<std::mutex> lock(mu_);
    return rpc_queue_.size();
}

void Worker::doCallRPC() {
    while (true) {
        AsyncRPCTask *task = nullptr;
        {
            std::unique_lock<std::mutex> lock(mu_);
            while (!stopped_ && rpc_queue_.empty()) {
                cond_.wait(lock);
            }
            if (stopped_) return;
            task = rpc_queue_.front();
            rpc_queue_.pop();
        }
        assert(task != nullptr);
        auto s = (task->call_func)();
        if (!s.ok()) {
            FLOG_ERROR("[Master] do %s rpc call failed: %s.",
                       AsyncCallTypeName(task->type).c_str(),
                       s.ToString().c_str());
        }
        delete task;
    }
}

void Worker::doNodeHeartbeat(TaskHandler *handler) {
    FLOG_INFO("[Master] NodeHeartbeat thread start.");
    while (true) {
        mspb::NodeHeartbeatRequest req;
        handler->CollectNodeHeartbeat(&req);
        AsyncNodeHeartbeat(req);
        // 等待被stop或者下次心跳时间到
        std::unique_lock<std::mutex> lock(mu_);
        if (cond_.wait_for(lock, std::chrono::seconds(node_heartbeat_secs_),
                           [this]() { return stopped_; })) {
            FLOG_INFO("[Master] NodeHeartbeat thread end.");
            return;
        }
    }
}

Status Worker::GetRaftAddress(uint64_t node_id, std::string *addr) {
    return client_->GetNodeAddress(node_id, nullptr, addr, nullptr);
}

Status Worker::GetServerAddress(uint64_t node_id, std::string *addr) {
    return client_->GetNodeAddress(node_id, addr, nullptr, nullptr);
}

bool Worker::pushCall(AsyncRPCTask *task) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) {
            return false;
        }
        rpc_queue_.push(task);
    }
    cond_.notify_all();
    return true;
}

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
