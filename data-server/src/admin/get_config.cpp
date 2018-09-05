#include "admin_server.h"

#include <map>
#include <string>
#include <functional>

#include <fastcommon/logger.h>
#include <common/ds_config.h>

#include "common/ds_config.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

using namespace ds_adminpb;

using ConfigGeterMap = std::map<std::string, std::function<std::string()>> ;

#define ADD_CFG_GETTER(section, name) \
    {#section"."#name, [] { return std::to_string(ds_config.section##_config.name); }}

#define ADD_CFG_GETTER_STR(section, name) \
    {#section"."#name, [] { return std::string(ds_config.section##_config.name); }}

static const ConfigGeterMap cfg_getters = {
        // log
        {"log.level", []{ return std::string(get_log_level_name(g_log_context.log_level)); }},
        {"log.log_level", []{ return std::string(get_log_level_name(g_log_context.log_level)); }},

        // rocksdb
        ADD_CFG_GETTER_STR(rocksdb, path),
        ADD_CFG_GETTER(rocksdb, block_cache_size),
        ADD_CFG_GETTER(rocksdb, row_cache_size),
        ADD_CFG_GETTER(rocksdb, block_size),
        ADD_CFG_GETTER(rocksdb, max_open_files),
        ADD_CFG_GETTER(rocksdb, bytes_per_sync),
        ADD_CFG_GETTER(rocksdb, write_buffer_size),
        ADD_CFG_GETTER(rocksdb, max_write_buffer_number),
        ADD_CFG_GETTER(rocksdb, min_write_buffer_number_to_merge),
        ADD_CFG_GETTER(rocksdb, max_bytes_for_level_base),
        ADD_CFG_GETTER(rocksdb, max_bytes_for_level_multiplier),
        ADD_CFG_GETTER(rocksdb, target_file_size_base),
        ADD_CFG_GETTER(rocksdb, target_file_size_multiplier),
        ADD_CFG_GETTER(rocksdb, max_background_flushes),
        ADD_CFG_GETTER(rocksdb, max_background_compactions),
        ADD_CFG_GETTER(rocksdb, background_rate_limit),
        ADD_CFG_GETTER(rocksdb, disable_auto_compactions),
        ADD_CFG_GETTER(rocksdb, read_checksum),
        ADD_CFG_GETTER(rocksdb, level0_file_num_compaction_trigger),
        ADD_CFG_GETTER(rocksdb, level0_slowdown_writes_trigger),
        ADD_CFG_GETTER(rocksdb, level0_stop_writes_trigger),
        ADD_CFG_GETTER(rocksdb, disable_wal),
        ADD_CFG_GETTER(rocksdb, cache_index_and_filter_blocks),
        ADD_CFG_GETTER(rocksdb, compression),
        ADD_CFG_GETTER(rocksdb, storage_type),
        ADD_CFG_GETTER(rocksdb, min_blob_size),
        ADD_CFG_GETTER(rocksdb, blob_file_size),
        ADD_CFG_GETTER(rocksdb, enable_garbage_collection),
        ADD_CFG_GETTER(rocksdb, blob_gc_percent),
        ADD_CFG_GETTER(rocksdb, blob_compression),
        ADD_CFG_GETTER(rocksdb, blob_ttl_range),
        ADD_CFG_GETTER(rocksdb, blob_cache_size),
        ADD_CFG_GETTER(rocksdb, ttl),
        ADD_CFG_GETTER(rocksdb, enable_stats),
        ADD_CFG_GETTER(rocksdb, enable_debug_log),

        // range
        ADD_CFG_GETTER(range, recover_skip_fail),
        ADD_CFG_GETTER(range, recover_concurrency),
        ADD_CFG_GETTER(range, check_size),
        ADD_CFG_GETTER(range, split_size),
        ADD_CFG_GETTER(range, max_size),
        ADD_CFG_GETTER(range, worker_threads),
        ADD_CFG_GETTER(range, access_mode),

        // raft
        ADD_CFG_GETTER(raft, port),
        ADD_CFG_GETTER_STR(raft, log_path),
        ADD_CFG_GETTER(raft, log_file_size),
        ADD_CFG_GETTER(raft, max_log_files),
        ADD_CFG_GETTER(raft, allow_log_corrupt),
        ADD_CFG_GETTER(raft, consensus_threads),
        ADD_CFG_GETTER(raft, consensus_queue),
        ADD_CFG_GETTER(raft, apply_threads),
        ADD_CFG_GETTER(raft, apply_queue),
        ADD_CFG_GETTER(raft, transport_send_threads),
        ADD_CFG_GETTER(raft, transport_recv_threads),
        ADD_CFG_GETTER(raft, tick_interval_ms),
        ADD_CFG_GETTER(raft, max_msg_size),

        // metric
        ADD_CFG_GETTER(metric, interval),

        // worker
        ADD_CFG_GETTER_STR(worker, ip_addr),
        ADD_CFG_GETTER(worker, port),
        ADD_CFG_GETTER(worker, accept_threads),
        ADD_CFG_GETTER(worker, event_recv_threads),
        ADD_CFG_GETTER(worker, event_send_threads),
        ADD_CFG_GETTER(worker, recv_buff_size),
        {"worker.fast_worker", [] { return std::to_string(ds_config.fast_worker_num); }},
        {"worker.slow_worker", [] { return std::to_string(ds_config.slow_worker_num); }},

        // manager
        ADD_CFG_GETTER_STR(manager, ip_addr),
        ADD_CFG_GETTER(manager, port),
        ADD_CFG_GETTER(manager, accept_threads),
        ADD_CFG_GETTER(manager, event_recv_threads),
        ADD_CFG_GETTER(manager, event_send_threads),
        ADD_CFG_GETTER(manager, recv_buff_size),

        // heartbeat
        {"heartbeat.master_num", []{ return std::to_string(ds_config.hb_config.master_num); }},
        {"heartbeat.node_heartbeat_interval", []{ return std::to_string(ds_config.hb_config.node_interval); }},
        {"heartbeat.range_heartbeat_interval", []{ return std::to_string(ds_config.hb_config.range_interval); }},
        {"heartbeat.master_host", []{
            std::string result;
            for (int i = 0; i < ds_config.hb_config.master_num; i++) {
                if (ds_config.hb_config.master_host[i][0] != '\0') {
                    result += ds_config.hb_config.master_host[i];
                    result += ",";
                }
            }
            if (!result.empty()) result.pop_back();
            return result;
        }},
};

static bool getConfigValue(const ConfigKey& key, std::string* value) {
    FLOG_DEBUG("[Admin] get config: %s.%s", key.section().c_str(), key.name().c_str());

    auto it = cfg_getters.find(key.section() + "." + key.name());
    if (it != cfg_getters.end()) {
        *value = (it->second)();
        return true;
    } else {
        return false;
    };
}

Status AdminServer::getConfig(const ds_adminpb::GetConfigRequest& req, ds_adminpb::GetConfigResponse* resp) {
    for (const auto& key: req.key()) {
        auto cfg = resp->add_configs();
        cfg->mutable_key()->CopyFrom(key);
        if(!getConfigValue(key, cfg->mutable_value())) {
            return Status(Status::kNotFound, "config key", key.section() + "." + key.name());
        }
    }
    return Status::OK();
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
