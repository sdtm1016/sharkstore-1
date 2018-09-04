//
// Created by 丁俊 on 2018/7/27.
//

#ifndef SHARKSTORE_DS_DS_CONF_H
#define SHARKSTORE_DS_DS_CONF_H
const std::string confStr="base_path = /tmp/sharkstore\n\
\n\
# time unit: ms\n\
# set task defaul timeout if request is not\n\
# default value is 3000 ms\n\
task_timeout = 3000\n\
\n\
# unix group name to run this program,\n\
# not set (empty) means run by the group of current user\n\
run_by_group =\n\
\n\
# unix username to run this program,\n\
# not set (empty) means run by current user\n\
run_by_user =\n\
\n\
[rocksdb]\n\
\n\
# rocksdb path\n\
path = /tmp/sharkstore/db\n\
\n\
# rocksdb block cache size, default 1024MB, max uint: MB\n\
# block_cache_size = 1024MB\n\
\n\
# rocksdb row cache size, default 0MB, max uint: MB\n\
# row_cache_size = 0MB\n\
\n\
# default: 16KB\n\
# block_size = 16KB\n\
\n\
# default: 1024\n\
# max_open_files = 1024\n\
\n\
# default: 1MB\n\
# bytes_per_sync = 1MB\n\
\n\
# default: 512MB\n\
# write_buffer_size = 512MB\n\
\n\
# default: 16\n\
# max_write_buffer_number = 16\n\
\n\
# default: 1\n\
# min_write_buffer_number_to_merge = 1\n\
\n\
# default: 512MB\n\
# max_bytes_for_level_base = 512MB\n\
\n\
# default: 10\n\
# max_bytes_for_level_multiplier = 10\n\
\n\
# default: 128MB\n\
# target_file_size_base = 128MB\n\
\n\
# default: 1\n\
# target_file_size_multiplier = 1\n\
\n\
# default: 1\n\
# max_background_flushes = 1\n\
\n\
# default: 32\n\
# max_background_compactions = 32\n\
\n\
#default:true\n\
#read_checksum = true\n\
\n\
# default: 8\n\
# level0_file_num_compaction_trigger = 8\n\
\n\
# default: 40\n\
# level0_slowdown_writes_trigger = 40\n\
\n\
# default: 46\n\
# level0_stop_writes_trigger = 46\n\
\n\
# set to 1 disable wal. default: 0\n\
# disable_wal = 0\n\
\n\
# set to 1 open cache_index_and_filter_blocks. default: 0\n\
# cache_index_and_filter_blocks = 0\n\
\n\
#use blob storage;default:0,blob:1\n\
#storage_type = 0\n\
\n\
# db ttl, seconds. default: 0(no ttl)\n\
# ttl = 0\n\
\n\
#min_blob_size default:0\n\
min_blob_size = 256\n\
\n\
#enable_garbage_collection default:false\n\
#enable_garbage_collection = 0\n\
\n\
[heartbeat]\n\
\n\
# master's ip_addr and port\n\
# may be multiple different master\n\
master_host = 127.0.0.1:7080\n\
\n\
# the number of the above master_host\n\
master_num = 1\n\
\n\
# time unit: s\n\
# default value is 10 ms\n\
node_heartbeat_interval = 10\n\
\n\
# time unit: s\n\
# default value is 10 s\n\
range_heartbeat_interval = 10\n\
\n\
\n\
[log]\n\
\n\
#if log path is not set then use base_path\n\
#log path = $log_path + /logs\n\
log_path= /tmp/sharkstore\n\
\n\
# sync log buff to disk every interval seconds\n\
# default value is 10 seconds\n\
sync_log_buff_interval = 10\n\
\n\
# if rotate the error log every day\n\
# default value is false\n\
rotate_error_log = true\n\
\n\
# keep days of the log files\n\
# 0 means do not delete old log files\n\
# default value is 0\n\
log_file_keep_days = 7\n\
\n\
#standard log level as syslog, case insensitive, value list:\n\
### emerg for emergency\n\
### alert\n\
### crit for critical\n\
### error\n\
### warn for warning\n\
### notice\n\
### info\n\
### debug\n\
log_level=info\n\
\n\
[socket]\n\
# connect timeout in seconds\n\
# default value is 30s\n\
connect_timeout = 3\n\
\n\
# network timeout in seconds\n\
# default value is 30s\n\
network_timeout = 30\n\
\n\
# epoll wait timeout\n\
# default value is 30ms\n\
epoll_timeout = 30\n\
\n\
#socket keep time\n\
#default value is 30m\n\
socket_keep_time = 1800\n\
\n\
# max concurrent connections this server supported\n\
# default value is 256\n\
max_connections = 100000\n\
\n\
# default value is 16K\n\
max_pkg_size = 256KB\n\
\n\
# default value is 64KB\n\
min_buff_size = 16KB\n\
\n\
# default value is 64KB\n\
max_buff_size = 256KB\n\
\n\
\n\
[worker]\n\
\n\
#ip_addr = 127.0.0.1\n\
\n\
# listen port of recv data\n\
port = 6180\n\
\n\
# socket accept thread number\n\
# default value is 1\n\
accept_threads = 1\n\
\n\
# epoll recv event thread number\n\
# no default value and must be configured\n\
event_recv_threads = 4\n\
\n\
# epoll send event thread number\n\
# no default value and must be configured\n\
event_send_threads = 2\n\
\n\
# thread only handle fast tasks. eg. RawGet\n\
fast_worker = 4\n\
\n\
# thread only handle slow tasks. eg. select\n\
slow_worker = 8\n\
\n\
# default value is min_buff_size of socket section\n\
recv_buff_size = 64KB\n\
\n\
[manager]\n\
\n\
#ip_addr = 127.0.0.1\n\
\n\
# listen port of recv data\n\
port = 16180\n\
\n\
# socket accept thread number\n\
# default value is 1\n\
accept_threads = 1\n\
\n\
# epoll recv event thread number\n\
# no default value and must be configured\n\
event_recv_threads = 1\n\
\n\
# epoll send event thread number\n\
# no default value and must be configured\n\
event_send_threads = 1\n\
\n\
# the number of threads dealing with the recved queue\n\
# no default value and must be configured\n\
worker_threads = 2\n\
\n\
# default value is min_buff_size of socket section\n\
#recv_buff_size = 64KB\n\
\n\
[range]\n\
\n\
# the range real_size is calculated\n\
# if statis_size is greater than check_size\n\
# default value is 32MB\n\
check_size = 32MB\n\
\n\
# range split threshold\n\
# default value is 64MB\n\
split_size = 64MB\n\
\n\
# default value is 128MB\n\
max_size = 128MB\n\
\n\
# range real size statis thread num\n\
worker_threads = 1\n\
\n\
# 0 sql, 1 redis, default=0\n\
access_mode = 0\n\
\n\
[raft]\n\
\n\
# ports used by the raft protocol\n\
port = 18887\n\
\n\
#raft log path\n\
log_path = /tmp/sharkstore/raft\n\
\n\
# log_file_size = 16777216\n\
# max_log_files = 5\n\
\n\
# consensus_threads = 4\n\
# consensus_queue = 100000\n\
\n\
# apply_threads = 4\n\
# apply_queue = 100000\n\
\n\
# grpc_send_threads = 4\n\
# grpc_recv_threads = 4\n\
\n\
# 单位ms\n\
# tick_interval = 500\n\
\n\
# max size per msg\n\
# max_msg_size = 1024 * 1024\n\
\n\
# default 1 (yes)\n\
# allow_log_corrupt = 1\n\
\n\
[metric]\n\
\n\
# metric report ip\n\
ip_addr = 10.12.142.23\n\
\n\
# metric report port\n\
port = 8887\n\
\n\
# epoll send event thread number\n\
# no default value and must be configured\n\
event_send_threads = 1\n\
\n\
# metric report interval\n\
# default value is 60s\n\
interval = 60\n\
\n\
# which cluster to belong to\n\
cluster_id = 1;\n\
\n\
#metric report name_space\n\
name_space = ds\n\
\n\
#metric report uri\n\
uri = /metric/tcp/process\n\
\n\
[client]\n\
\n\
ip_addr = 127.0.0.6\n\
port = 6180\n\
\n\
event_recv_threads = 1\n\
event_send_threads = 1\n\
worker_threads = 0\n";
#endif //SHARKSTORE_DS_DS_CONF_H
