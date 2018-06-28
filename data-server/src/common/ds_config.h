#ifndef __DS_CONFIG_H__
#define __DS_CONFIG_H__

#include <limits.h>
#include "frame/sf_config.h"

typedef struct ds_config_s {
    int fast_worker_num;  // fast worker thread num; eg. put/get command
    int slow_worker_num;  // fast worker thread num; eg. put/get command

    int task_timeout;  // defualt 3,000ms

    struct {
        char path[PATH_MAX];
        size_t block_cache_size; // default: 1024MB
        size_t row_cache_size;
        size_t block_size; // default: 16K
        int max_open_files;
        size_t bytes_per_sync;
        size_t write_buffer_size;
        int max_write_buffer_number;
        int min_write_buffer_number_to_merge;
        size_t max_bytes_for_level_base;
        int max_bytes_for_level_multiplier;
        size_t target_file_size_base;
        int target_file_size_multiplier;
        int max_background_flushes;
        int max_background_compactions;
        bool read_checksum;
        int level0_file_num_compaction_trigger;
        int level0_slowdown_writes_trigger;
        int level0_stop_writes_trigger;
        bool disable_wal;
        bool cache_index_and_filter_blocks;
        int ttl;
    } rocksdb_config;

    struct {
        int node_interval;   // node heartbeat interval
        int range_interval;  // range heartbeat interval
        int master_num;      // master server num
        char **master_host;  // master server host:port
    } hb_config;

    struct {
        uint64_t check_size;
        uint64_t split_size;
        uint64_t max_size;
        int worker_threads;
        int access_mode; // 0 sql, 1 redis, default=0
    } range_config;

    struct {
        int port;  // raft server port
        char log_path[PATH_MAX];
        size_t log_file_size;
        size_t max_log_files;
        int allow_log_corrupt;
        size_t consensus_threads;
        size_t consensus_queue;
        size_t apply_threads;
        size_t apply_queue;
        size_t transport_send_threads;
        size_t transport_recv_threads;
        size_t tick_interval_ms;
        size_t max_msg_size;
    } raft_config;

    struct {
        int cluster_id;
        int interval;
        int port;
        char ip_addr[16];
        char name_space[32];
        char uri[256];
    } metric_config;

    sf_socket_thread_config_t manager_config;  // manager thread config
    sf_socket_thread_config_t worker_config;   // worker thread config
    sf_socket_thread_config_t client_config;   // metric client config
} ds_config_t;

extern ds_config_t ds_config;

#ifdef __cplusplus
extern "C" {
#endif

int load_from_conf_file(IniContext *ini_context, const char *filename);

void print_rocksdb_config();

#ifdef __cplusplus
}
#endif

#endif  //__DS_CONFIG_H__
