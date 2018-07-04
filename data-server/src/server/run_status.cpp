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

run_status_t g_status;

namespace sharkstore {
namespace dataserver {
namespace server {

std::atomic<uint64_t> RunStatus::fs_usage_percent_(0);

int RunStatus::Init(ContextServer *context) {
    context_ = context;
    socket_client_ = new common::SocketClient(context->socket_session);

    strcpy(ds_config.client_config.thread_name_prefix, "metric");
    if (socket_client_->Init(&ds_config.client_config, &metric_status_) != 0) {
        FLOG_ERROR("RunStatus (socket client) Init error ...");
        return -1;
    }
    return 0;
}

int RunStatus::Start() {
    FLOG_INFO("RunStatus Start begin ...");
    if (socket_client_->Start() != 0) {
        FLOG_ERROR("RunStatus Start error ...");
        return -1;
    }

    metric_thread_ = std::thread(&RunStatus::Send, this);

    auto handle = metric_thread_.native_handle();
    AnnotateThread(handle, "metric_hb");

    // set monitor version
    system_status_.PutVersion(get_version());

    return 0;
}

void RunStatus::Stop() {
    FLOG_INFO("RunStatus Stop begin ...");

    cond_.notify_all();

    if (socket_client_ != nullptr) {
        socket_client_->Stop();
        delete socket_client_;
        socket_client_ = nullptr;
    }

    if (metric_thread_.joinable()) {
        metric_thread_.join();
    }
}

std::string RunStatus::GetSubSystem() {
    if (sub_system_.empty()) {
        auto node_id = context_->node_id;
        context_->master_worker->GetServerAddress(node_id, &sub_system_);
    }

    return sub_system_;
}

std::string RunStatus::GetMetric() {
    if (metric_info_.empty()) {
        std::string metric = "GET ";

        metric += ds_config.metric_config.uri;

        metric += "?clusterId=";
        metric += std::to_string(ds_config.metric_config.cluster_id);

        metric += "&namespace=";
        metric += ds_config.metric_config.name_space;

        metric += "&subsystem=";
        std::string uri = GetSubSystem();

        char buff[512];
        int len = 0;

        metric += urlencode(uri.c_str(), uri.length(), buff, &len);
        metric += " HTTP/1.1\r\n";
        metric += "Host: ";
        metric += ds_config.metric_config.ip_addr;
        metric += ":" + std::to_string(ds_config.metric_config.port);
        metric += "\r\nUser-Agent: sharkstore-ds-client/1.1\r\n\r\n";

        metric_info_ = std::move(metric);
    }

    return metric_info_;
}

void RunStatus::Send() {
    while (g_continue_flag) {
        bool is_first;
        int64_t session_id;

        std::tie(session_id, is_first) = socket_client_->get_session_id(
            ds_config.metric_config.ip_addr, ds_config.metric_config.port);

        if (is_first) {
            FLOG_DEBUG("session: %" PRId64 " first report env metric",
                       session_id);
            auto metric = GetMetric();
            SendBuff(session_id, metric);
        }
        if (session_id > 0) {
            FLOG_DEBUG("session: %" PRId64 " report process statics metric",
                       session_id);
            std::string metric;
            system_status_.GetProcessStats(metric);
            SendBuff(session_id, metric, sizeof(int));
        } else {
            FLOG_ERROR("run status connect error");
        }

        printDBMetric();
        context_->worker->PrintQueueSize();

        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait_for(lock,
                       std::chrono::seconds(ds_config.metric_config.interval));
    }
}

void RunStatus::SendBuff(int64_t session_id, std::string &metric,
                         int header_len) {
    int buff_len = header_len + metric.length();
    response_buff_t *resp = new_response_buff(buff_len);
    resp->session_id = session_id;

    if (header_len > 0) {
        int2buff(metric.length(), resp->buff);
    }

    memcpy(resp->buff + header_len, metric.c_str(), metric.length());
    resp->buff_len = buff_len;

    socket_client_->Send(resp);
}

void RunStatus::SetHardDiskInfo() {
    monitor::HardDiskInfo hdi;
    system_status_.GetHardDiskInfo(hdi);

    uint64_t total = 0, available = 0;
    if (system_status_.GetFileSystemUsage(ds_config.rocksdb_config.path, &total,
                                          &available)) {
        if (total > 0 && available <= total) {
            g_status.hard_info.total_size = total;
            g_status.hard_info.free_size = available;
            g_status.hard_info.used_size = total - available;

            fs_usage_percent_ = (total - available) * 100 / total;

        } else {
            FLOG_ERROR("collect harddisk usage error(invalid size: %lu:%lu) ",
                       total, available);
        }
    } else {
        FLOG_ERROR("collect harddisk usage error: %s", strErrno(errno).c_str());
    }
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
              "block-cache=%lu, pinned=%lu",
              tr_mem_usage.c_str(), mem_table_usage.c_str(),
              context_->block_cache->GetUsage(),
              context_->block_cache->GetPinnedUsage());
    //    auto status =
    //    context_->rocks_db->CompactRange(rocksdb::CompactRangeOptions(),
    //    nullptr, nullptr);
    //    if (!status.ok()) {
    //        FLOG_ERROR("compact db failed: %s", status.ToString().c_str());
    //    }
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
