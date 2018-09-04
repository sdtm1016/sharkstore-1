#include "ds_config.h"

#include <fastcommon/common_define.h>
#include <fastcommon/ini_file_reader.h>
#include <fastcommon/shared_func.h>

#include "frame/sf_logger.h"
#include "frame/sf_util.h"

#include "ds_define.h"
#include "ds_version.h"

ds_config_t ds_config;

// 加载非负带字节单位的配置项（MB、KB等)
static size_t load_bytes_value_ne(IniContext *ini_context, const char *section, const char *item,
                                  size_t default_value) {
    int64_t value = default_value;
    char *temp_str = iniGetStrValue(section, item, ini_context);
    if (temp_str == NULL) {
        return default_value;
    } else {
        int ret = parse_bytes(temp_str, 1, &value);
        if (ret != 0 || value < 0) {
            fprintf(stderr, "[ds config] parse option %s:%s failed.\n\n", section, item);
            exit(-1);
        } else {
            return (size_t)value;
        }
    }
}

static int load_integer_value_atleast(IniContext *ini_context, const char *section, const char *item,
                                      int default_value, int atleast_value) {
    int value = iniGetIntValue(section, item, ini_context, default_value);
    if (value < atleast_value) {
        fprintf(stderr, "[ds config] invalid options %s:%s. value=%d, at least=%d", section, item, value, atleast_value);
        exit(-1);
    }
    return value;
}

static int load_rocksdb_config(IniContext *ini_context) {
    char *section = "rocksdb";

    // db路径
    char *temp_str = iniGetStrValue(section, "path", ini_context);
    if (temp_str != NULL) {
        snprintf(ds_config.rocksdb_config.path, sizeof(ds_config.rocksdb_config.path), "%s", temp_str);
    } else {
        fprintf(stderr, "[ds config] rockdb path is missing");
        return -1;
    }

    ds_config.rocksdb_config.block_cache_size =
            load_bytes_value_ne(ini_context, section, "block_cache_size", 1024 * 1024 * 1024);

    ds_config.rocksdb_config.row_cache_size =
            load_bytes_value_ne(ini_context, section, "row_cache_size", 0);

    ds_config.rocksdb_config.block_size =
            load_bytes_value_ne(ini_context, section, "block_size", 16 * 1024);

    ds_config.rocksdb_config.max_open_files =
            load_integer_value_atleast(ini_context, section, "max_open_files", 1024, 100);

    ds_config.rocksdb_config.bytes_per_sync =
            load_bytes_value_ne(ini_context, section, "bytes_per_sync", 0);

    ds_config.rocksdb_config.write_buffer_size =
            load_bytes_value_ne(ini_context, section, "write_buffer_size", 128 << 20);

    ds_config.rocksdb_config.max_write_buffer_number =
            load_integer_value_atleast(ini_context, section, "max_write_buffer_number", 8, 2);

    ds_config.rocksdb_config.min_write_buffer_number_to_merge =
            load_integer_value_atleast(ini_context, section, "min_write_buffer_number_to_merge", 1, 1);

    ds_config.rocksdb_config.max_bytes_for_level_base =
            load_bytes_value_ne(ini_context, section, "max_bytes_for_level_base", 512 << 20);
    ds_config.rocksdb_config.max_bytes_for_level_multiplier =
            load_integer_value_atleast(ini_context, section, "max_bytes_for_level_multiplier", 10, 1);

    ds_config.rocksdb_config.target_file_size_base =
            load_bytes_value_ne(ini_context, section, "target_file_size_base", 128 << 20);
    ds_config.rocksdb_config.target_file_size_multiplier =
            load_integer_value_atleast(ini_context, section, "target_file_size_multiplier", 1, 1);

    ds_config.rocksdb_config.max_background_flushes =
            load_integer_value_atleast(ini_context, section, "max_background_flushes", 2, 1);
    ds_config.rocksdb_config.max_background_compactions =
            load_integer_value_atleast(ini_context, section, "max_background_compactions", 4, 1);
    ds_config.rocksdb_config.background_rate_limit =
            load_bytes_value_ne(ini_context, section, "background_rate_limit", 0);

    ds_config.rocksdb_config.disable_auto_compactions =
            (bool)iniGetIntValue(section, "disable_auto_compactions", ini_context, 0);

    ds_config.rocksdb_config.read_checksum =
            (bool)iniGetIntValue(section, "read_checksum", ini_context, 1);

    ds_config.rocksdb_config.level0_file_num_compaction_trigger =
            load_integer_value_atleast(ini_context, section, "level0_file_num_compaction_trigger", 8, 1);
    ds_config.rocksdb_config.level0_slowdown_writes_trigger =
            load_integer_value_atleast(ini_context, section, "level0_slowdown_writes_trigger", 40, 1);
    ds_config.rocksdb_config.level0_stop_writes_trigger =
            load_integer_value_atleast(ini_context, section, "level0_stop_writes_trigger", 46, 1);

    ds_config.rocksdb_config.disable_wal =
            (bool)iniGetIntValue(section, "disable_wal", ini_context, 0);

    ds_config.rocksdb_config.cache_index_and_filter_blocks =
            (bool)iniGetIntValue(section, "cache_index_and_filter_blocks", ini_context, 0);

    ds_config.rocksdb_config.compression = load_integer_value_atleast(ini_context, section, "compression", 0, 0);

    ds_config.rocksdb_config.storage_type = load_integer_value_atleast(ini_context, section, "storage_type", 0, 0);

    ds_config.rocksdb_config.min_blob_size = load_integer_value_atleast(ini_context, section, "min_blob_size", 0, 0);
    ds_config.rocksdb_config.blob_file_size =  load_bytes_value_ne(ini_context, section, "blob_file_size", 256 * 1024 * 1024UL);
    ds_config.rocksdb_config.enable_garbage_collection =
            (bool)iniGetIntValue(section, "enable_garbage_collection", ini_context, 1);

    ds_config.rocksdb_config.blob_gc_percent = load_integer_value_atleast(ini_context, section, "blob_gc_percent", 75, 10);
    if (ds_config.rocksdb_config.blob_gc_percent > 100) {
        fprintf(stderr, "invalid rocksdb blob_gc_percent config(%d)", ds_config.rocksdb_config.blob_gc_percent);
        return -1;
    }
    ds_config.rocksdb_config.blob_compression = load_integer_value_atleast(ini_context, section, "blob_compression", 0, 0);

    ds_config.rocksdb_config.ttl = load_integer_value_atleast(ini_context, section, "ttl", 0, 0);

    ds_config.rocksdb_config.enable_stats =
            (bool)iniGetIntValue(section, "enable_stats",ini_context, 1);

    ds_config.rocksdb_config.enable_debug_log =
            (bool)iniGetIntValue(section, "enable_debug_log",ini_context, 0);

    return 0;
}

void print_rocksdb_config() {
    FLOG_INFO("rockdb_configs: "
              "\n\tpath: %s"
              "\n\tblock_cache_size: %lu"
              "\n\trow_cache_size: %lu"
              "\n\tblock_size: %lu"
              "\n\tmax_open_files: %d"
              "\n\tbytes_per_sync: %lu"
              "\n\twrite_buffer_size: %lu"
              "\n\tmax_write_buffer_number: %d"
              "\n\tmin_write_buffer_number_to_merge: %d"
              "\n\tmax_bytes_for_level_base: %lu"
              "\n\tmax_bytes_for_level_multiplier: %d"
              "\n\ttarget_file_size_base: %lu"
              "\n\ttarget_file_size_multiplier: %d"
              "\n\tmax_background_flushes: %d"
              "\n\tmax_background_compactions: %d"
              "\n\tbackground_rate_limit: %lu"
              "\n\tdisable_auto_compactions: %d"
              "\n\tread_checksum: %d"
              "\n\tlevel0_file_num_compaction_trigger: %d"
              "\n\tlevel0_slowdown_writes_trigger: %d"
              "\n\tlevel0_stop_writes_trigger: %d"
              "\n\tdisable_wal: %d"
              "\n\tcache_index_and_filter_blocks: %d"
              "\n\tcompression: %d"
              "\n\tstorage_type: %d"
              "\n\tmin_blob_size: %d"
              "\n\tblob_file_size: %lu"
              "\n\tenable_garbage_collection: %d"
              "\n\tblob_gc_percent: %d"
              "\n\tblob_compression: %d"
              "\n\tttl: %d"
              "\n\tenable_stats: %d"
              "\n\tenable_debug_log: %d"
              ,
              ds_config.rocksdb_config.path,
              ds_config.rocksdb_config.block_cache_size,
              ds_config.rocksdb_config.row_cache_size,
              ds_config.rocksdb_config.block_size,
              ds_config.rocksdb_config.max_open_files,
              ds_config.rocksdb_config.bytes_per_sync,
              ds_config.rocksdb_config.write_buffer_size,
              ds_config.rocksdb_config.max_write_buffer_number,
              ds_config.rocksdb_config.min_write_buffer_number_to_merge,
              ds_config.rocksdb_config.max_bytes_for_level_base,
              ds_config.rocksdb_config.max_bytes_for_level_multiplier,
              ds_config.rocksdb_config.target_file_size_base,
              ds_config.rocksdb_config.target_file_size_multiplier,
              ds_config.rocksdb_config.max_background_flushes,
              ds_config.rocksdb_config.max_background_compactions,
              ds_config.rocksdb_config.background_rate_limit,
              ds_config.rocksdb_config.disable_auto_compactions,
              ds_config.rocksdb_config.read_checksum,
              ds_config.rocksdb_config.level0_file_num_compaction_trigger,
              ds_config.rocksdb_config.level0_slowdown_writes_trigger,
              ds_config.rocksdb_config.level0_stop_writes_trigger,
              ds_config.rocksdb_config.disable_wal,
              ds_config.rocksdb_config.cache_index_and_filter_blocks,
              ds_config.rocksdb_config.compression,
              ds_config.rocksdb_config.storage_type,
              ds_config.rocksdb_config.min_blob_size,
              ds_config.rocksdb_config.blob_file_size,
              ds_config.rocksdb_config.enable_garbage_collection,
              ds_config.rocksdb_config.blob_gc_percent,
              ds_config.rocksdb_config.blob_compression,
              ds_config.rocksdb_config.ttl,
              ds_config.rocksdb_config.enable_stats,
              ds_config.rocksdb_config.enable_debug_log
              );
}

static int load_range_config(IniContext *ini_context) {
    int mega = 1024 * 1024;

    int result;
    char *temp_char;
    int64_t temp_int;

    char *section = "range";

    ds_config.range_config.worker_threads =
        iniGetIntValue(section, "worker_threads", ini_context, 1);
    if (ds_config.range_config.worker_threads == 0) {
        ds_config.range_config.worker_threads = 1;
    }

    ds_config.range_config.recover_skip_fail =
            (bool)iniGetIntValue(section, "recover_skip_fail", ini_context, 1);

    ds_config.range_config.recover_concurrency =
            load_integer_value_atleast(ini_context, section, "recover_concurrency", 8, 1);

    ds_config.range_config.access_mode =
        iniGetIntValue(section, "access_mode", ini_context, 0);
    if (ds_config.range_config.access_mode != 0 && ds_config.range_config.access_mode != 1) {
        ds_config.range_config.access_mode = 0;
    }

    temp_char = iniGetStrValue(section, "check_size", ini_context);
    if (temp_char == NULL) {
        temp_int = 32 * mega;
    } else if ((result = parse_bytes(temp_char, 1, &temp_int)) != 0) {
        return result;
    }

    ds_config.range_config.check_size = temp_int;

    temp_char = iniGetStrValue(section, "split_size", ini_context);
    if (temp_char == NULL) {
        temp_int = 64 * mega;
    } else if ((result = parse_bytes(temp_char, 1, &temp_int)) != 0) {
        return result;
    }

    ds_config.range_config.split_size = temp_int;

    temp_char = iniGetStrValue(section, "max_size", ini_context);
    if (temp_char == NULL) {
        temp_int = 96 * mega;
    } else if ((result = parse_bytes(temp_char, 1, &temp_int)) != 0) {
        return result;
    }

    ds_config.range_config.max_size = temp_int;

    if (ds_config.range_config.check_size >= ds_config.range_config.split_size) {
        FLOG_ERROR("load range config error, valid config: "
                   "check_size < split_size; ");

        return -1;
    }
    if (ds_config.range_config.split_size >= ds_config.range_config.max_size) {
            FLOG_ERROR("load range config error, valid config: "
                       "split_size < max_size; ");

            return -1;
        }

    return 0;
}

static int load_raft_config(IniContext *ini_context) {
    static const uint16_t kDefaultPort = 6182;
    static const size_t kDefaultLogFileSize = 1024 * 1024 * 16;
    static const size_t kDefaultMaxLogFiles = 5;

    char *temp_str;
    char *section = "raft";

    ds_config.raft_config.port = iniGetIntValue(section, "port", ini_context, 6182);
    if (ds_config.raft_config.port <= 0) {
        ds_config.raft_config.port = kDefaultPort;
    }

    temp_str = iniGetStrValue(section, "log_path", ini_context);
    if (temp_str != NULL) {
        snprintf(ds_config.raft_config.log_path, sizeof(ds_config.raft_config.log_path),
                 "%s", temp_str);
    } else {
        strcpy(ds_config.raft_config.log_path, "/tmp/ds/raft");
    }

    ds_config.raft_config.log_file_size = load_bytes_value_ne(
            ini_context, section, "log_file_size", kDefaultLogFileSize);

    ds_config.raft_config.max_log_files = (size_t)load_integer_value_atleast(
            ini_context, section, "max_log_files", kDefaultMaxLogFiles, 2);

    ds_config.raft_config.allow_log_corrupt =
         iniGetIntValue(section, "allow_log_corrupt", ini_context, 1);

    ds_config.raft_config.consensus_threads = (size_t)load_integer_value_atleast(
            ini_context, section, "consensus_threads", 4, 1);
    ds_config.raft_config.consensus_queue = (size_t)load_integer_value_atleast(
            ini_context, section , "consensus_queue", 100000, 100);

    ds_config.raft_config.apply_threads = (size_t)load_integer_value_atleast(
            ini_context, section, "apply_threads", 4, 1);
    ds_config.raft_config.apply_queue = (size_t)load_integer_value_atleast(
            ini_context, section, "apply_queue", 100000, 100);

    ds_config.raft_config.transport_send_threads = (size_t)load_integer_value_atleast(
            ini_context, section, "transport_send_threads", 4, 1);
    ds_config.raft_config.transport_recv_threads = (size_t)load_integer_value_atleast(
            ini_context, section, "transport_recv_threads", 4, 1);

    ds_config.raft_config.tick_interval_ms = (size_t)load_integer_value_atleast(
           ini_context, section, "tick_interval", 500, 100);

    ds_config.raft_config.max_msg_size =
        load_bytes_value_ne(ini_context, section, "max_msg_size", 1024 * 1024);

    return 0;
}

void print_raft_config() {
    FLOG_INFO("raft configs: "
              "\n\tport: %d"
              "\n\tpath: %s"
              "\n\tlog_file_size: %lu"
              "\n\tmax_log_files: %lu"
              "\n\tallow_log_corrupt: %d"
              "\n\tconsensus_threads: %lu"
              "\n\tconsensus_queue: %lu"
              "\n\tapply_threads: %lu"
              "\n\tapply_queue: %lu"
              "\n\tsend_threads: %lu"
              "\n\trecv_threads: %lu"
              "\n\ttick_interval_ms: %lu"
              "\n\tmax_msg_size: %lu"
              ,
              ds_config.raft_config.port,
              ds_config.raft_config.log_path,
              ds_config.raft_config.log_file_size,
              ds_config.raft_config.max_log_files,
              ds_config.raft_config.allow_log_corrupt,
              ds_config.raft_config.consensus_threads,
              ds_config.raft_config.consensus_queue,
              ds_config.raft_config.apply_threads,
              ds_config.raft_config.apply_queue,
              ds_config.raft_config.transport_send_threads,
              ds_config.raft_config.transport_recv_threads,
              ds_config.raft_config.tick_interval_ms,
              ds_config.raft_config.max_msg_size
    );
}

static int load_metric_config(IniContext *ini_context) {
    char *section = "metric";

    ds_config.metric_config.interval =
        iniGetIntValue(section, "interval", ini_context, 10);
    if (ds_config.metric_config.interval <= 0) {
        ds_config.metric_config.interval = 10;
    }
    return 0;
}

static int load_watch_config(IniContext *ini_context) {
    char *section = "watch";

    ds_config.watch_config.buffer_map_size =
            iniGetIntValue(section, "buffer_map_size", ini_context, 10);
    if (ds_config.watch_config.buffer_map_size <= 0) {
        ds_config.watch_config.buffer_map_size = 10;
    }

    ds_config.watch_config.buffer_queue_size =
            iniGetIntValue(section, "buffer_queue_size", ini_context, 100);
    if (ds_config.watch_config.buffer_queue_size <= 0) {
        ds_config.watch_config.buffer_queue_size = 100;
    }

    ds_config.watch_config.watcher_set_size =
            iniGetIntValue(section, "watcher_set_size", ini_context, 8);
    if (ds_config.watch_config.buffer_queue_size <= 0) {
        ds_config.watch_config.buffer_queue_size = 8;
    }

    return 0;
}


static int load_heartbeat_config(IniContext *ini_context) {
    char *section = "heartbeat";

    ds_config.hb_config.node_interval =
        iniGetIntValue(section, "node_interval", ini_context, 10);
    if (ds_config.hb_config.node_interval <= 0) {
        ds_config.hb_config.node_interval = 10;
    }

    ds_config.hb_config.range_interval =
        iniGetIntValue(NULL, "range_interval", ini_context, 10);
    if (ds_config.hb_config.range_interval <= 0) {
        ds_config.hb_config.range_interval = 10;
    }

    ds_config.hb_config.master_num =
        iniGetIntValue(section, "master_num", ini_context, 3);
    if (ds_config.hb_config.master_num <= 0) {
        ds_config.hb_config.master_num = 3;
    }

    ds_config.hb_config.master_host =
        malloc(sizeof(char *) * ds_config.hb_config.master_num);

    char **temp = malloc(sizeof(char *) * ds_config.hb_config.master_num);

    int result = iniGetValues(section, "master_host", ini_context, temp,
                              ds_config.hb_config.master_num);

    if (result <= 0) {
        fprintf(stderr, "[ds config] load master host config failed\n");
        free(temp);
        return -1;
    }

    for (int i = 0; i < ds_config.hb_config.master_num; i++) {
        ds_config.hb_config.master_host[i] = malloc(32);
        strcpy(ds_config.hb_config.master_host[i], temp[i]);
    }

    free(temp);

    return 0;
}

static int load_woker_num_config(IniContext *ini_context) {
    char *section = "worker";

    ds_config.fast_worker_num = iniGetIntValue(section, "fast_worker", ini_context, 4);
    if (ds_config.fast_worker_num < 0) {
        ds_config.fast_worker_num = 4;
    }

    ds_config.slow_worker_num = iniGetIntValue(section, "slow_worker", ini_context, 8);
    if (ds_config.slow_worker_num < 0) {
        ds_config.slow_worker_num = 8;
    }

    return 0;
}

int load_from_conf_file(IniContext *ini_context, const char *filename) {
    int result = 0;

    result =
        sf_load_socket_thread_config(ini_context, "manager", &ds_config.manager_config);
    if (result != 0) {
        return result;
    }

    result =
        sf_load_socket_thread_config(ini_context, "worker", &ds_config.worker_config);
    if (result != 0) {
        return result;
    }

    if (load_woker_num_config(ini_context) != 0) {
        return -1;
    }

    if (load_heartbeat_config(ini_context) != 0) {
        return -1;
    }

    if (load_rocksdb_config(ini_context) != 0) {
        return -1;
    }

    if (load_range_config(ini_context) != 0) {
        return -1;
    }

    if (load_raft_config(ini_context) != 0) {
        return -1;
    }

    if (load_metric_config(ini_context) != 0) {
        return -1;
    }

    if(load_watch_config(ini_context) != 0) {
        return -1;
    }

    ds_config.task_timeout = iniGetIntValue(NULL, "task_timeout", ini_context, 3000);
    if (ds_config.task_timeout <= 0) {
        ds_config.task_timeout = 3000;
    }

    return 0;
}
