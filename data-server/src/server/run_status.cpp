#include "run_status.h"

#include <chrono>
#include <thread>

#include <fastcommon/shared_func.h>

#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_version.h"
#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "master/worker.h"

#include "server.h"
#include "worker.h"

namespace sharkstore {
namespace dataserver {
namespace server {

int RunStatus::Init(ContextServer *context) {
    context_ = context;
    return 0;
}

int RunStatus::Start() {
    FLOG_INFO("RunStatus Start begin ...");

    metric_thread_ = std::thread(&RunStatus::run, this);
    auto handle = metric_thread_.native_handle();
    AnnotateThread(handle, "metric_hb");

    // set monitor version
    system_status_.PutVersion(get_version());

    return 0;
}

void RunStatus::Stop() {
    FLOG_INFO("RunStatus Stop begin ...");

    cond_.notify_all();

    if (metric_thread_.joinable()) {
        metric_thread_.join();
    }
}

void RunStatus::run() {
    while (g_continue_flag) {
        collectDiskUsage();
        printDBMetric();
        context_->worker->PrintQueueSize();
        printStatistics();

        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait_for(lock,
                       std::chrono::seconds(ds_config.metric_config.interval));
    }
}

bool RunStatus::GetFilesystemUsage(FileSystemUsage* usage) {
    uint64_t total = 0, available = 0;
    if (system_status_.GetFileSystemUsage(ds_config.rocksdb_config.path, &total,
                                          &available)) {
        if (total > 0 && available <= total) {
            usage->total_size = total;
            usage->free_size = available;
            usage->used_size = total - available;
            return true;
        } else {
            FLOG_ERROR("collect filesystem usage error(invalid size: %" PRIu64 ":%" PRIu64 ") ",
                    total, available);
            return false;
        }
    } else {
        FLOG_ERROR("collect filesystem usage error: %s", strErrno(errno).c_str());
        return false;
    }
}

void RunStatus::ReportLeader(uint64_t range_id, bool is_leader) {
    std::lock_guard<std::mutex> lock(leaders_mu_);
    if (is_leader) {
        leaders_.insert(range_id);
    } else {
        leaders_.erase(range_id);
    }
}

uint64_t RunStatus::GetLeaderCount() const {
    std::lock_guard<std::mutex> lock(leaders_mu_);
    return leaders_.size();
}

// 定时采集磁盘使用率
void RunStatus::collectDiskUsage() {
    FileSystemUsage usage;
    if (GetFilesystemUsage(&usage)) {
        fs_usage_percent_ = usage.used_size * 100/ usage.total_size;
    }
}

void RunStatus::printStatistics() {
    FLOG_INFO("\n%s", statistics_.ToString().c_str());
    statistics_.Reset();
}

void RunStatus::printDBMetric() {
    assert(context_->rocks_db != nullptr);
    assert(context_->block_cache != nullptr);
    auto db = context_->rocks_db;
    std::string tr_mem_usage;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem_usage);
    std::string mem_table_usage;
    db->GetProperty("rocksdb.cur-size-all-mem-tables", &mem_table_usage);
    FLOG_INFO("rocksdb memory usages: table-readers=%s, memtables=%s, "
              "block-cache=%lu, pinned=%lu, row-cache=%lu",
              tr_mem_usage.c_str(), mem_table_usage.c_str(),
              context_->block_cache->GetUsage(),
              context_->block_cache->GetPinnedUsage(),
              (context_->row_cache ? context_->row_cache->GetUsage() : 0));

    auto stat = context_->db_stats;
    if (stat) {
        FLOG_INFO("rocksdb row-cache stats: hit=%" PRIu64 ", miss=%" PRIu64,
                  stat->getAndResetTickerCount(rocksdb::ROW_CACHE_HIT),
                  stat->getAndResetTickerCount(rocksdb::ROW_CACHE_MISS));

        FLOG_INFO("rocksdb block-cache stats: hit=%" PRIu64 ", miss=%" PRIu64,
                  stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_HIT),
                  stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_MISS));

        FLOG_INFO("rockdb get histograms: %s", stat->getHistogramString(rocksdb::DB_GET).c_str());
        FLOG_INFO("rockdb write histograms: %s", stat->getHistogramString(rocksdb::DB_WRITE).c_str());
        stat->Reset();
    }
}


}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
