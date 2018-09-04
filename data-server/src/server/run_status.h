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
#include "monitor/statistics.h"
#include "range/stats.h"

#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

struct FileSystemUsage {
    uint64_t total_size = 0;
    uint64_t used_size = 0;
    uint64_t free_size = 0;
};

class RunStatus : public range::RangeStats {
public:
    RunStatus() = default;
    ~RunStatus() = default;

    RunStatus(const RunStatus &) = delete;
    RunStatus &operator=(const RunStatus &) = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();

    void PushTime(monitor::HistogramType type, int64_t time) override {
        if (time > 0) statistics_.PushTime(type, static_cast<uint64_t>(time));
    }

    bool GetFilesystemUsage(FileSystemUsage* usage);
    uint64_t GetFilesystemUsedPercent() const { return fs_usage_percent_.load();}

    void ReportLeader(uint64_t range_id, bool is_leader) override;
    uint64_t GetLeaderCount() const;

    void IncrSplitCount() override { ++split_count_; }
    void DecrSplitCount() override { --split_count_; }
    uint64_t GetSplitCount() const { return split_count_; }

private:
    void run();
    void collectDiskUsage();
    void printStatistics();
    void printDBMetric();

private:
    ContextServer *context_ = nullptr;

    monitor::ISystemStatus system_status_;
    monitor::Statistics statistics_;

    std::atomic<uint64_t> fs_usage_percent_ = {0};
    std::atomic<uint64_t> split_count_ = {0};

    std::set<uint64_t> leaders_;
    mutable std::mutex leaders_mu_;

    std::mutex mutex_;
    std::condition_variable cond_;
    std::thread metric_thread_;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
#endif  //__RUN_STATUS_H__
