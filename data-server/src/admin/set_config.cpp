#include "admin_server.h"

#include <fastcommon/shared_func.h>
#include "frame/sf_logger.h"
#include "common/ds_config.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

using namespace ds_adminpb;

using ConfigSetFunc = std::function<Status(server::ContextServer*, const std::string&)>;
using ConfigSetFuncMap = std::map<std::string, ConfigSetFunc>;

static Status setLogLevel(server::ContextServer* ctx, const std::string& value) {
    (void)ctx;
    char level[8] = {'\0'};
    snprintf(level, 8, "%s", value.c_str());
    set_log_level(level);
    return Status::OK();
}

#define SET_RANGE_SIZE(opt) \
    {"range."#opt, [](server::ContextServer *ctx, const std::string& value) { \
        (void)ctx; \
        uint64_t new_value = 0; \
        try { \
            new_value = std::stoull(value); \
        } catch (std::exception &e) { \
            return Status(Status::kInvalidArgument, "range "#opt, value); \
        } \
        ds_config.range_config.opt = new_value; \
        return Status::OK(); \
    }}

#define SET_ROCKSDB_OPTIONS(opt) \
    {"rocksdb."#opt, [](server::ContextServer *ctx, const std::string& value) { \
        auto db = ctx->rocks_db; \
        auto s = db->SetOptions(db->DefaultColumnFamily(), {{#opt, value}}); \
        if (!s.ok()) { \
            return Status(Status::kIOError, "SetOptions", s.ToString()); \
        } else { \
            return Status::OK(); \
        } \
    }}

#define SET_ROCKSDB_DBOPTIONS(opt) \
    {"rocksdb."#opt, [](server::ContextServer *ctx, const std::string& value) { \
        auto db = ctx->rocks_db; \
        auto s = db->SetDBOptions({{#opt, value}}); \
        if (!s.ok()) { \
            return Status(Status::kIOError, "SetDBOptions", s.ToString()); \
        } else { \
            return Status::OK(); \
        } \
    }}


static const ConfigSetFuncMap cfg_set_funcs = {
        // log level
        {"log.level",        setLogLevel},
        {"log.log_level",    setLogLevel},

        // range split
        SET_RANGE_SIZE(check_size),
        SET_RANGE_SIZE(split_size),
        SET_RANGE_SIZE(max_size),

        // rocksdb configs
        SET_ROCKSDB_OPTIONS(disable_auto_compactions),
        SET_ROCKSDB_OPTIONS(write_buffer_size),
        SET_ROCKSDB_OPTIONS(max_write_buffer_number),
        SET_ROCKSDB_OPTIONS(soft_pending_compaction_bytes_limit),
        SET_ROCKSDB_OPTIONS(hard_pending_compaction_bytes_limit),
        SET_ROCKSDB_OPTIONS(level0_file_num_compaction_trigger),
        SET_ROCKSDB_OPTIONS(level0_slowdown_writes_trigger),
        SET_ROCKSDB_OPTIONS(level0_stop_writes_trigger),
        SET_ROCKSDB_OPTIONS(target_file_size_base),
        SET_ROCKSDB_OPTIONS(target_file_size_multiplier),
        SET_ROCKSDB_OPTIONS(max_bytes_for_level_base),
        SET_ROCKSDB_OPTIONS(max_bytes_for_level_multiplier),
        SET_ROCKSDB_OPTIONS(report_bg_io_stats),
        SET_ROCKSDB_OPTIONS(paranoid_file_checks),
        SET_ROCKSDB_OPTIONS(compression),
        SET_ROCKSDB_DBOPTIONS(max_background_jobs),
        SET_ROCKSDB_DBOPTIONS(base_background_compactions),
        SET_ROCKSDB_DBOPTIONS(max_background_compactions),
        SET_ROCKSDB_DBOPTIONS(avoid_flush_during_shutdown),
        SET_ROCKSDB_DBOPTIONS(delayed_write_rate),
        SET_ROCKSDB_DBOPTIONS(max_total_wal_size),
        SET_ROCKSDB_DBOPTIONS(delete_obsolete_files_period_micros),
        SET_ROCKSDB_DBOPTIONS(stats_dump_period_sec),
        SET_ROCKSDB_DBOPTIONS(max_open_files),
        SET_ROCKSDB_DBOPTIONS(bytes_per_sync),
        SET_ROCKSDB_DBOPTIONS(wal_bytes_per_sync),
        SET_ROCKSDB_DBOPTIONS(compaction_readahead_size),
};

Status AdminServer::setConfig(const SetConfigRequest& req, SetConfigResponse* resp) {
    for (auto &cfg: req.configs()) {
        auto it = cfg_set_funcs.find(cfg.key().section() + "." + cfg.key().name());
        if (it == cfg_set_funcs.end()) {
            return Status(Status::kNotSupported, "set config",
                    cfg.key().section() + "." + cfg.key().name());
        }
        auto s = (it->second)(context_, cfg.value());
        if (!s.ok()) {
            FLOG_WARN("[Admin] config %s.%s set to %s failed: %s", cfg.key().section().c_str(),
                      cfg.key().name().c_str(), cfg.value().c_str(), s.ToString().c_str());
            return s;
        }
        FLOG_INFO("[Admin] config %s.%s set to %s.", cfg.key().section().c_str(),
                cfg.key().name().c_str(), cfg.value().c_str());
    }
    return Status::OK();
}

} // namespace admin
} // namespace dataserver
} // namespace sharkstore

