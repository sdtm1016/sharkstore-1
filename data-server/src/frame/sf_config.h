#ifndef __SF_CONFIG_H__
#define __SF_CONFIG_H__

#include <stdbool.h>
#include <time.h>

#include <fastcommon/common_define.h>
#include <fastcommon/ini_file_reader.h>

typedef int (*sf_load_config_callback_t)(IniContext *ini, const char *filename);

typedef struct sf_log_config_s {
    bool rotate_error_log;
    int log_file_keep_days;
    int sync_log_buff_interval;
    char log_path[MAX_PATH_SIZE];
    char log_level[8];
} sf_log_config_t;

typedef struct sf_socket_thread_config_s {
    char ip_addr[IP_ADDRESS_SIZE];  // host ip addr
    int port;                       // host port

    int accept_threads;      // accept thread count
    int event_recv_threads;  // event loop thread for recv
    int event_send_threads;  // event loop thread for send
    int worker_threads;      // worker thread count
    int recv_buff_size;      // recv socket buff size

    pthread_t *accept_tids;  // accept thread ids
    pthread_t *recv_tids;    // recv thread ids
    pthread_t *send_tids;    // send thread ids

    char thread_name_prefix[8];
} sf_socket_thread_config_t;

typedef struct sf_socket_config_s {
    int max_pkg_size;     // socket package max size
    int min_buff_size;    // socket task buff init size
    int max_buff_size;    // socket task buff max size
    int max_connections;  // max socket connection count

    int connect_timeout;   // connect timeout
    int network_timeout;   // network timeout
    int epoll_timeout;     // epoll wait timeout
    int socket_keep_time;  // socket keep alive time
} sf_socket_config_t;

typedef struct sf_run_config_s {
    gid_t run_by_gid;
    uid_t run_by_uid;

    char run_by_group[32];
    char run_by_user[32];
} sf_run_config_t;

typedef struct sf_config_s {
    sf_socket_config_t socket_config;
    sf_run_config_t run_config;
    sf_log_config_t log_config;
} sf_config_t;

extern sf_config_t sf_config;
extern char g_base_path[MAX_PATH_SIZE];

extern volatile bool g_continue_flag;
extern time_t g_up_time;

#ifdef __cplusplus
extern "C" {
#endif
int sf_load_config_buffer(const char* content,const char* server_name);
int sf_load_config(const char *server_name, const char *filename);
int sf_load_socket_thread_config(IniContext *ini_context, const char *section_name,
                                 sf_socket_thread_config_t *config);

void sf_set_load_config_callback(sf_load_config_callback_t load_config_func);

#ifdef __cplusplus
}
#endif

#endif  //__SF_CONFIG_H__
