#include "sf_service.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef USE_GPERF
#include <gperftools/profiler.h>
#endif

#include <fastcommon/ini_file_reader.h>
#include <fastcommon/ioevent_loop.h>
#include <fastcommon/process_ctrl.h>
#include <fastcommon/pthread_func.h>
#include <fastcommon/sched_thread.h>
#include <fastcommon/shared_func.h>
#include <fastcommon/sockopt.h>

#include "sf_logger.h"

static bool is_terminate_flag = false;

static void sig_quit_handler(int sig);
static void sig_hup_handler(int sig);
static void sig_usr_handler(int sig);

#if defined(DEBUG_FLAG)
static void sig_dump_handler(int sig);
#endif

static int sf_setup_signal_handler();

static int setup_schedule_tasks();

#define ALLOC_CONNECTIONS_ONCE 256

static pthread_t schedule_tid;
static char *conf_filename = NULL;
static int sf_task_arg_size = sizeof(sf_task_arg_t);

static pthread_mutex_t service_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t service_cond = PTHREAD_COND_INITIALIZER;

static sf_user_init_callback_t sf_user_init_callback;
static sf_user_destroy_callback_t sf_user_destroy_callback;
static sf_print_version_callback_t sf_print_version_callback;

void sf_regist_print_version_callback(sf_print_version_callback_t print_version_func) {
    sf_print_version_callback = print_version_func;
}

void sf_regist_user_init_callback(sf_user_init_callback_t user_init_func) {
    sf_user_init_callback = user_init_func;
}

void sf_regist_user_destroy_callback(sf_user_destroy_callback_t user_destroy_func) {
    sf_user_destroy_callback = user_destroy_func;
}

int sf_service_init() {
    int result;
    int init_count;

    if ((result = set_rand_seed()) != 0) {
        FLOG_ERROR("set_rand_seed fail, program exit!");
        return result;
    }
    sf_socket_config_t *config = &sf_config.socket_config;

    init_count = config->max_connections < ALLOC_CONNECTIONS_ONCE
                     ? config->max_connections
                     : ALLOC_CONNECTIONS_ONCE;

    if ((result = free_queue_init_ex(config->max_connections, init_count,
                                     ALLOC_CONNECTIONS_ONCE, config->min_buff_size,
                                     config->max_buff_size, sf_task_arg_size)) != 0) {
        return result;
    }

    if (sf_user_init_callback != NULL) {
        if ((result = sf_user_init_callback()) != 0) {
            return result;
        }
    }

    return 0;
}

void sf_service_destroy() {
    if (sf_user_destroy_callback != NULL) {
        sf_user_destroy_callback();
    }

    // free resource
    free_queue_destroy();
}

static void usage(const char *program) {
    fprintf(stderr, "Usage: %s <config_file> [start | restart [debug] | stop ]\n",
            program);
}

static void servicer_loop() {
    struct timespec ts;
    while (g_continue_flag) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 5;

        pthread_mutex_lock(&service_mutex);
        pthread_cond_timedwait(&service_cond, &service_mutex, &ts);
        pthread_mutex_unlock(&service_mutex);
    }
}

int sf_service_run(int argc, char *argv[], const char *server_name) {
    int result;
    char pid_filename[MAX_PATH_SIZE];
    bool stop;

    if (argc < 2) {
        usage(argv[0]);
        return 1;
    } else if (argc == 2) {
        if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0) {
            sf_print_version_callback();
            return 0;
        }
    }

    g_current_time = time(NULL);
    g_up_time = g_current_time;
    srand(g_up_time);

    log_init2();
    log_set_time_precision(&g_log_context, LOG_TIME_PRECISION_MSECOND);

    conf_filename = argv[1];
    if ((result = get_base_path_from_conf_file(conf_filename, g_base_path,
                                               sizeof(g_base_path))) != 0) {
        log_destroy();
        return result;
    }

    snprintf(pid_filename, sizeof(pid_filename), "%s/%s.pid", g_base_path, server_name);
    if ((result = process_action(pid_filename, argv[2], &stop)) != 0) {
        if (result == EINVAL) {
            usage(server_name);
        }
        log_destroy();
        return result;
    }
    if (stop) {
        log_destroy();
        return 0;
    }

    do {
        if (argc < 4 || strcmp(argv[3], "debug") != 0) {
            daemon_init(false);
            umask(0);
        }

        if ((result = write_to_pid_file(pid_filename)) != 0) {
            break;
        }

        if ((result = sf_load_config(server_name, conf_filename)) != 0) {
            break;
        }

        if ((result = sf_setup_signal_handler()) != 0) {
            break;
        }

        // sf_set_remove_from_ready_list(false);
        if ((result = sf_service_init()) != 0) {
            break;
        }

        if ((result = setup_schedule_tasks()) != 0) {
            break;
        }

        log_set_cache(true);

    } while (0);

    if (result != 0) {
        FLOG_CRIT("exit abnormally!\n");
        log_destroy();
        return result;
    }

    servicer_loop();

    if (g_schedule_flag) {
        pthread_kill(schedule_tid, SIGINT);
    }
    sf_service_destroy();

    delete_pid_file(pid_filename);

    FLOG_INFO("exit normally.\n");

    log_destroy();
    return 0;
}


int sf_service_run_test(const char* confStr) {
    int result;
    //char pid_filename[MAX_PATH_SIZE];
    //bool stop;

    g_current_time = time(NULL);
    g_up_time = g_current_time;
    srand(g_up_time);

    log_init2();
    log_set_time_precision(&g_log_context, LOG_TIME_PRECISION_MSECOND);

    do {

        daemon_init(false);
        umask(0);

        if ((result = sf_load_config_buffer(confStr,"ds-test")) != 0) {
            break;
        }

        if ((result = sf_setup_signal_handler()) != 0) {
            break;
        }

        if ((result = sf_service_init()) != 0) {
            break;
        }

        if ((result = setup_schedule_tasks()) != 0) {
            break;
        }

        log_set_cache(true);

    } while (0);

    if (result != 0) {
        FLOG_CRIT("exit abnormally!\n");
        log_destroy();
        return result;
    }

    servicer_loop();

    if (g_schedule_flag) {
        pthread_kill(schedule_tid, SIGINT);
    }
    sf_service_destroy();


    FLOG_INFO("exit normally.\n");

    log_destroy();
    return 0;
}

static int setup_schedule_tasks() {
#define SCHEDULE_ENTRIES_COUNT 4

    ScheduleEntry schedule_entries[SCHEDULE_ENTRIES_COUNT];
    ScheduleArray schedule_array;
    ScheduleEntry *entry;
    int count = 0;

    entry = schedule_entries;
    memset(schedule_entries, 0, sizeof(schedule_entries));

    entry->id = ++count;
    entry->time_base.hour = TIME_NONE;
    entry->time_base.minute = TIME_NONE;
    entry->interval = sf_config.log_config.sync_log_buff_interval;
    entry->task_func = log_sync_func;
    entry->func_args = &g_log_context;
    entry++;

    if (sf_config.log_config.rotate_error_log) {
        log_set_rotate_time_format(&g_log_context, "%Y%m%d");

        entry->id = ++count;
        entry->time_base.hour = 23;
        entry->time_base.minute = 59;
        entry->time_base.second = 30;
        entry->interval = 86400;
        entry->task_func = log_notify_rotate;
        entry->func_args = &g_log_context;
        entry++;

        if (sf_config.log_config.log_file_keep_days > 0) {
            log_set_keep_days(&g_log_context, sf_config.log_config.log_file_keep_days);

            entry->id = ++count;
            entry->time_base.hour = 1;
            entry->time_base.minute = 0;
            entry->time_base.second = 0;
            entry->interval = 86400;
            entry->task_func = log_delete_old_files;
            entry->func_args = &g_log_context;
            entry++;
        }
    }

    schedule_array.entries = schedule_entries;
    schedule_array.count = count;
    return sched_start(&schedule_array, &schedule_tid, 1024 * 1024,
                       (bool *volatile) & g_continue_flag);
}

#if defined(DEBUG_FLAG)
static void sig_dump_handler(int sig) {
    static bool is_dump_flag = false;
    char filename[256];

    if (is_dump_flag) {
        return;
    }

    is_dump_flag = true;

    snprintf(filename, sizeof(filename), "%s/logs/sf_dump.log", g_base_path);
    // manager_dump_global_vars_to_file(filename);

    is_dump_flag = false;
}
#endif

static void sig_quit_handler(int sig) {
    if (!is_terminate_flag) {
        is_terminate_flag = true;
        g_continue_flag = false;
        FLOG_CRIT("catch signal %d, program exiting...", sig);
    }

    pthread_cond_signal(&service_cond);
}

static void sig_hup_handler(int sig) { FLOG_INFO("catch signal %d", sig); }

static void sig_usr_handler(int sig) {
    if (sig == SIGUSR1) {
#ifdef USE_GPERF
        static bool prof_trigger = false;
        if (!prof_trigger) {
            ProfilerStart("/tmp/ds.pprof");
        } else {
            ProfilerStop();
        }
        prof_trigger = !prof_trigger;
#endif
    } else if (sig == SIGUSR2) {
        static bool log_debug_trigger = false;
        if (!log_debug_trigger) {
            char level[8] = "debug";
            set_log_level(level);
        } else {
            set_log_level(sf_config.log_config.log_level);
        }
        log_debug_trigger = !log_debug_trigger;
    } else {
        FLOG_INFO("catch signal %d, ignore it", sig);
    }
}

static int sf_setup_signal_handler() {
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);

    act.sa_handler = sig_usr_handler;
    if (sigaction(SIGUSR1, &act, NULL) < 0 || sigaction(SIGUSR2, &act, NULL) < 0) {
        FLOG_CRIT("call sigaction fail, errno: %d, error info: %s", errno,
                  strerror(errno));
        FLOG_CRIT("exit abnormally!\n");
        return errno;
    }

    act.sa_handler = sig_hup_handler;
    if (sigaction(SIGHUP, &act, NULL) < 0) {
        FLOG_CRIT("call sigaction fail, errno: %d, error info: %s", errno,
                  strerror(errno));
        FLOG_CRIT("exit abnormally!\n");
        return errno;
    }

    act.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &act, NULL) < 0) {
        FLOG_CRIT("call sigaction fail, errno: %d, error info: %s", errno,
                  strerror(errno));
        FLOG_CRIT("exit abnormally!\n");
        return errno;
    }

    act.sa_handler = sig_quit_handler;
    if (sigaction(SIGINT, &act, NULL) < 0 || sigaction(SIGTERM, &act, NULL) < 0 ||
        sigaction(SIGQUIT, &act, NULL) < 0) {
        FLOG_CRIT("call sigaction fail, errno: %d, error info: %s", errno,
                  strerror(errno));
        FLOG_CRIT("exit abnormally!\n");
        return errno;
    }

#if defined(DEBUG_FLAG)
    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);
    act.sa_handler = sig_dump_handler;
    if (sigaction(SIGUSR1, &act, NULL) < 0 || sigaction(SIGUSR2, &act, NULL) < 0) {
        FLOG_CRIT("call sigaction fail, errno: %d, error info: %s", errno,
                  strerror(errno));
        FLOG_CRIT("exit abnormally!\n");
        return errno;
    }
#endif
    return 0;
}
