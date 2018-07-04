#ifndef __RUN_STATUS_H__
#define __RUN_STATUS_H__

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>

#include "common/socket_client.h"
#include "frame/sf_status.h"
#include "monitor/isystemstatus.h"
#include "monitor/syscommon.h"

#include "context_server.h"

typedef struct range_status_s {
    std::atomic<int> assigned_worker_threads;
    std::atomic<int> actual_worker_threads;

    std::atomic<uint32_t> range_count{0};
    std::atomic<uint32_t> range_leader_count{0};
    std::atomic<uint32_t> range_split_count{0};
} range_status_t;

typedef struct hard_info_s {
    uint64_t total_size = 0;
    uint64_t used_size = 0;
    uint64_t free_size = 0;
} hard_info_t;

typedef struct grpc_status_s {
    std::atomic<uint64_t> send_queue_size;
} grpc_status_t;

typedef struct run_status_s {
    sf_socket_status_t *worker_socket_status;
    sf_socket_status_t *manager_socket_status;
    sf_socket_status_t *client_socket_status;

    range_status_t range_status;
    grpc_status_t grpc_status;

    hard_info_t hard_info;
} run_status_t;

extern run_status_t g_status;

namespace sharkstore {
namespace dataserver {
namespace server {

class RunStatus {
public:
    RunStatus() = default;
    ~RunStatus() = default;

    RunStatus(const RunStatus &) = delete;
    RunStatus &operator=(const RunStatus &) = delete;
    RunStatus &operator=(const RunStatus &) volatile = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();

    void PushTime(monitor::PrintTag type, uint32_t time) {
        system_status_.PutTopData(type, time);
    }

    void PushRange(monitor::RangeTag type, uint64_t count) {
        system_status_.PutRangeData(type, count);
    }

    void SetHardDiskInfo();

private:
    std::string GetSubSystem();
    std::string GetMetric();

    void Send();
    void SendBuff(int64_t session_id, std::string &metric, int header_len = 0);

    // 打印rockdb统计信息
    void printDBMetric();

public:
    // 磁盘使用率百分比
    static std::atomic<uint64_t> fs_usage_percent_;

private:
    std::string sub_system_;
    std::string metric_info_;

    std::mutex mutex_;
    std::condition_variable cond_;

    std::thread metric_thread_;

    monitor::ISystemStatus system_status_;

    sf_socket_status_t metric_status_ = {0};

    common::SocketClient *socket_client_ = nullptr;
    ContextServer *context_ = nullptr;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
#endif  //__RUN_STATUS_H__
