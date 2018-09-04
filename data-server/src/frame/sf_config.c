#include "sf_config.h"

#include <sys/types.h>

#include <grp.h>
#include <pwd.h>

#include <fastcommon/ini_file_reader.h>
#include <fastcommon/shared_func.h>
#include <fastcommon/sockopt.h>

#include "sf_define.h"
#include "sf_logger.h"

// define server
volatile bool g_continue_flag = true;
time_t g_up_time = 0;

sf_config_t sf_config;

char g_base_path[MAX_PATH_SIZE] = {'/', 't', 'm', 'p', '\0'};

static sf_load_config_callback_t sf_load_config_callback;

void sf_set_load_config_callback(sf_load_config_callback_t load_config_func) {
    sf_load_config_callback = load_config_func;
}

static int sf_load_socket_config(IniContext *ini_context, sf_socket_config_t *config) {
    int result;

    char *section_name = "socket";
    char *min_buff_size;
    char *max_buff_size;
    char *temp_char;

    int64_t temp_int;

    config->connect_timeout = iniGetIntValue(section_name, "connect_timeout", ini_context,
                                             DEFAULT_CONNECT_TIMEOUT);
    if (config->connect_timeout <= 0) {
        config->connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    }

    config->network_timeout = iniGetIntValue(section_name, "network_timeout", ini_context,
                                             DEFAULT_NETWORK_TIMEOUT);
    if (config->network_timeout <= 0) {
        config->network_timeout = DEFAULT_NETWORK_TIMEOUT;
    }

    config->epoll_timeout = iniGetIntValue(section_name, "epoll_timeout", ini_context,
                                           DEFAULT_NETWORK_TIMEOUT);
    if (config->epoll_timeout <= 0) {
        config->epoll_timeout = DEFAULT_NETWORK_TIMEOUT;
    }

    config->socket_keep_time = iniGetIntValue(section_name, "socket_keep_time",
                                              ini_context, DEFAULT_SOCKET_KEEP_TIME);
    if (config->socket_keep_time <= 0) {
        config->socket_keep_time = DEFAULT_SOCKET_KEEP_TIME;
    }

    config->max_connections = iniGetIntValue(section_name, "max_connections", ini_context,
                                             DEFAULT_MAX_CONNECTONS);
    if (config->max_connections <= 0) {
        config->max_connections = DEFAULT_MAX_CONNECTONS;
    }

    temp_char = iniGetStrValue(section_name, "max_pkg_size", ini_context);
    if (temp_char == NULL) {
        temp_int = SF_DEF_MAX_PACKAGE_SIZE;
    } else if ((result = parse_bytes(temp_char, 1, &temp_int)) != 0) {
        return result;
    }
    config->max_pkg_size = (int)temp_int;

    min_buff_size = iniGetStrValue(section_name, "min_buff_size", ini_context);
    if (min_buff_size == NULL) {
        temp_int = SF_DEF_MIN_BUFF_SIZE;
    } else if ((result = parse_bytes(min_buff_size, 1, &temp_int)) != 0) {
        return result;
    }
    config->min_buff_size = (int)temp_int;

    max_buff_size = iniGetStrValue(section_name, "max_buff_size", ini_context);
    if (max_buff_size == NULL) {
        temp_int = SF_DEF_MAX_BUFF_SIZE;
    } else if ((result = parse_bytes(max_buff_size, 1, &temp_int)) != 0) {
        return result;
    }
    config->max_buff_size = (int)temp_int;

    if (min_buff_size == NULL || max_buff_size == NULL) {
        config->min_buff_size = config->max_pkg_size;
        config->max_buff_size = config->max_pkg_size;
    }

    if (config->max_buff_size < config->max_pkg_size) {
        config->max_buff_size = config->max_pkg_size;
    }

    return 0;
}

static int sf_load_log_config(IniContext *ini_context, const char *server_name,
                              sf_log_config_t *config) {
    int result;
    char *temp_char;

    char *section_name = "log";

    temp_char = iniGetStrValue(section_name, "log_path", ini_context);
    if (temp_char == NULL) {
        strcpy(config->log_path, g_base_path);
    } else {
        snprintf(config->log_path, sizeof(config->log_path), "%s", temp_char);
    }

    if ((result = log_set_prefix(config->log_path, server_name)) != 0) {
        return result;
    }

    config->rotate_error_log =
        iniGetBoolValue(section_name, "rotate_error_log", ini_context, false);
    config->log_file_keep_days =
        iniGetIntValue(section_name, "log_file_keep_days", ini_context, 0);

    config->sync_log_buff_interval = iniGetIntValue(
        section_name, "sync_log_buff_interval", ini_context, SYNC_LOG_BUFF_DEF_INTERVAL);
    if (config->sync_log_buff_interval <= 0) {
        config->sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
    }

    temp_char = iniGetStrValue(section_name, "log_level", ini_context);
    if (temp_char == NULL) {
        strcpy(config->log_level, "info");
    } else {
        snprintf(config->log_level, sizeof(config->log_level), "%s", temp_char);
    }

    set_log_level(config->log_level);

    return 0;
}

static int sf_load_run_config(IniContext *ini_context, sf_run_config_t *config) {
    int result;
    char *temp_char;

    temp_char = iniGetStrValue(NULL, "run_by_group", ini_context);
    if (temp_char == NULL) {
        *config->run_by_group = '\0';
    } else {
        snprintf(config->run_by_group, sizeof(config->run_by_group), "%s", temp_char);
    }

    if (*config->run_by_group != '\0') {
        struct group *run_by_group;

        run_by_group = getgrnam(config->run_by_group);
        if (run_by_group == NULL) {
            result = errno != 0 ? errno : ENOENT;
            FLOG_ERROR("getgrnam fail, errno: %d, error info: %s", result,
                       strerror(result));
            return result;
        }

        config->run_by_gid = run_by_group->gr_gid;
    } else {
        config->run_by_gid = getegid();
    }

    temp_char = iniGetStrValue(NULL, "run_by_user", ini_context);
    if (temp_char == NULL) {
        *config->run_by_user = '\0';
    } else {
        snprintf(config->run_by_user, sizeof(config->run_by_user), "%s", temp_char);
    }

    if (*config->run_by_user != '\0') {
        struct passwd *run_by_user;

        run_by_user = getpwnam(config->run_by_user);
        if (run_by_user == NULL) {
            result = errno != 0 ? errno : ENOENT;
            FLOG_ERROR("getpwnam fail, errno: %d, error info: %s", result,
                       strerror(result));
            return result;
        }

        config->run_by_uid = run_by_user->pw_uid;
    } else {
        config->run_by_uid = geteuid();
    }

    if ((result = set_run_by(config->run_by_group, config->run_by_user)) != 0) {
        return result;
    }

    return 0;
}

int sf_load_config(const char *server_name, const char *filename) {
    int result;

    IniContext ini_context;
    char full_filename[MAX_PATH_SIZE];

    snprintf(full_filename, sizeof(full_filename), "%s/%s", g_base_path, filename);

    memset(&ini_context, 0, sizeof(IniContext));
    if ((result = iniLoadFromFile(full_filename, &ini_context)) != 0) {
        FLOG_ERROR("load conf file \"%s\" fail, code: %d", filename, result);
        return result;
    }

    result = sf_load_log_config(&ini_context, server_name, &sf_config.log_config);
    if (result != 0) {
        return result;
    }

    result = sf_load_socket_config(&ini_context, &sf_config.socket_config);
    if (result != 0) {
        return result;
    }

    result = sf_load_run_config(&ini_context, &sf_config.run_config);
    if (result != 0) {
        return result;
    }

    if (sf_load_config_callback != NULL) {
        if ((result = sf_load_config_callback(&ini_context, filename)) != 0) {
            return result;
        }
    }

    iniFreeContext(&ini_context);
    return 0;
}


int sf_load_config_buffer(const char* content,const char* server_name) {
    int result;

    IniContext ini_context;

    memset(&ini_context, 0, sizeof(IniContext));
    ;
    if ((result = iniLoadFromBuffer(content, &ini_context)) != 0) {
        FLOG_ERROR("load conf file \"%s\" fail, code: %d", content, result);
        return result;
    }

    result = sf_load_log_config(&ini_context, server_name, &sf_config.log_config);
    if (result != 0) {
        return result;
    }

    result = sf_load_socket_config(&ini_context, &sf_config.socket_config);
    if (result != 0) {
        return result;
    }

    result = sf_load_run_config(&ini_context, &sf_config.run_config);
    if (result != 0) {
        return result;
    }

    if (sf_load_config_callback != NULL) {
        char* filename={'\0'};
        if ((result = sf_load_config_callback(&ini_context, filename)) != 0) {
            return result;
        }
    }

    iniFreeContext(&ini_context);
    return 0;
}

int sf_load_socket_thread_config(IniContext *ini_context, const char *section_name,
                                 sf_socket_thread_config_t *config) {
    char *temp_char;
    int64_t temp_int;

    config->worker_threads =
        iniGetIntValue(section_name, "worker_threads", ini_context, 0);
    if (config->worker_threads <= 0) {
        FLOG_WARN("section name: %s"
                  " item \"worker_threads\" is invalid, value: %d <= 0!",
                  section_name, config->worker_threads);
        config->worker_threads = 0;
    }

    config->accept_threads =
        iniGetIntValue(section_name, "accept_threads", ini_context, 0);
    if (config->accept_threads <= 0) {
        FLOG_WARN("section name: %s"
                  " item \"accept_threads\" is invalid, value: %d <= 0!",
                  section_name, config->accept_threads);
        config->accept_threads = 0;
    }

    config->event_recv_threads =
        iniGetIntValue(section_name, "event_recv_threads", ini_context, 0);
    if (config->event_recv_threads <= 0) {
        FLOG_WARN("section name: %s"
                  " item \"event_recv_threads\" is invalid, value: %d <= 0!",
                  section_name, config->event_recv_threads);
        config->event_recv_threads = 0;
    }

    config->event_send_threads =
        iniGetIntValue(section_name, "event_send_threads", ini_context, 0);
    if (config->event_send_threads <= 0) {
        FLOG_WARN("section name: %s"
                  " item \"event_send_threads\" is invalid, value: %d <= 0!",
                  section_name, config->event_send_threads);
        config->event_send_threads = 0;
    }

    config->port = iniGetIntValue(section_name, "port", ini_context, 0);
    if (config->port <= 0) {
        FLOG_ERROR("section name: %s"
                   " item \"listen_port\" is invalid, value: %d <= 0!",
                   section_name, config->port);
        return EINVAL;
    }

    temp_char = iniGetStrValue(section_name, "ip_addr", ini_context);
    if (temp_char == NULL) {
        config->ip_addr[0] = '\0';
        FLOG_WARN("section name: %s item \"ip_addr\" is null", section_name);
    } else {
        snprintf(config->ip_addr, sizeof(config->ip_addr), "%s", temp_char);
    }

    temp_char = iniGetStrValue(section_name, "recv_buff_size", ini_context);
    if (temp_char == NULL) {
        config->recv_buff_size = sf_config.socket_config.min_buff_size;
        return 0;
    } else if (parse_bytes(temp_char, 1, &temp_int) != 0) {
        config->recv_buff_size = sf_config.socket_config.min_buff_size;
        return 0;
    }

    config->recv_buff_size = (int)temp_int;

    return 0;
}
