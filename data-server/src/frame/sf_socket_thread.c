#include "sf_socket_thread.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>

#include <fastcommon/ioevent_loop.h>
#include <fastcommon/pthread_func.h>
#include <fastcommon/sched_thread.h>
#include <fastcommon/shared_func.h>
#include <fastcommon/sockopt.h>

#include "sf_util.h"
#include "sf_logger.h"
#include "sf_socket.h"

static int sf_accept_init(sf_socket_thread_t *context);
static int sf_recv_init(sf_socket_thread_t *context);
static int sf_send_init(sf_socket_thread_t *context);

static int sf_recv_task_done(struct fast_task_info *task);
static int sf_send_task_done(struct fast_task_info *task);
static int sf_enable_task_done(struct fast_task_info *task);

static int sf_socket_close(struct fast_task_info *task, int err);

static int64_t atomic_session_id = 0;

static struct fast_task_info *sf_init_task(sf_socket_thread_t *context,
        int sid, const char *ip_addr, const uint16_t port,
        sf_session_entry_t *session);

static struct fast_task_info *sf_clone_task(sf_socket_thread_t *context,
        const struct fast_task_info *rs);

int sf_socket_thread_init(sf_socket_thread_t *context) {
    int result;

    context->socket_close_callback  = sf_socket_close;
    context->recv_done_callback     = sf_recv_task_done;
    context->send_done_callback     = sf_send_task_done;
    context->enable_done_callback   = sf_enable_task_done;

    sf_socket_thread_config_t *config = context->socket_config;

    if ((result = sf_socket_session_init(&context->socket_session)) != 0) {
        return result;
    }

    if (!context->is_client) {
        result = sf_socket_server(config->ip_addr, config->port, &context->socket_fd);
        if (result != 0) {
            return result;
        }
    }

    if ((result = sf_recv_init(context)) != 0) {
        return result;
    }
    if ((result = sf_send_init(context)) != 0) {
        return result;
    }

    if (!context->is_client) {
        if ((result = sf_accept_init(context)) != 0) {
            return result;
        }
    }

    return 0;
}

int64_t sf_connect_session_get(sf_socket_thread_t *context,
        const char *ip_addr, const uint16_t port) {
    int sock = -1;
    int ret = -1;
    sf_session_entry_t *session;

    sf_socket_thread_config_t *config = context->socket_config;
    sf_socket_status_t *status = context->socket_status;

    if (config->event_send_threads <= 0) {
        FLOG_CRIT("socket client event_send_threads must be gt 0 !");
        return -1;
    }

    ret = sf_connect_to_server(ip_addr, port, &sock);
    if (ret != 0) {
        FLOG_ERROR("connect ip: %s, port: %d failed, error: %s",
                ip_addr, port, strerror(ret));
        return -1;
    }

    int64_t session_id = __sync_add_and_fetch(&atomic_session_id, 1);

    // map session_id => socket_fd
    session = sf_create_socket_session(&context->socket_session, session_id);
    session->state      = SS_INIT;

    do {
        session->stask = sf_init_task(context, sock, ip_addr, port, session);
        if (session->stask == NULL) {
            break;
        }

        //set send thread data
        session->stask->thread_data = context->event_send_data +
            sock % config->event_send_threads;

        __sync_fetch_and_add(&status->current_connections, 1);

        //connect set is_sending
        session->is_sending = true;

        if (sf_add_connect_notify(session->stask) != 0) {
            FLOG_ERROR("connect bind ip: %s, port: %d fd: %d"
                    " session_id: %" PRId64 " fail",
                    ip_addr, port, sock, session_id);

            break;
        }

        FLOG_DEBUG("connect bind ip: %s, port: %d fd: %d session_id: %" PRId64,
                ip_addr, port, sock, session_id);

        return session_id;
    } while (false);

    close(sock);
    sf_session_free(&context->socket_session, session_id);
    return -1;
}

static int sf_enable_task_done(struct fast_task_info *task) {
    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context = task_arg->context;
    sf_session_entry_t *session = task_arg->session;

    sf_socket_thread_config_t *config = context->socket_config;

    //set callback for send buf
    sf_set_send_callback(task);

    //business may not need to receive data.
    if (config->event_recv_threads > 0) {
        session->rtask = sf_clone_task(context, task);

        //set send thread data
        session->rtask->thread_data = context->event_recv_data +
            task->event.fd % config->event_recv_threads;

        if (sf_add_recv_notify(session->rtask) != 0) {
            FLOG_ERROR("ip:%s fd: %d session: %" PRId64 " set recv event fail",
                    task->client_ip, task->event.fd, session->session_id);
            return context->socket_close_callback(task, EIO);
        }
    }

    //set send buf
    if (sf_send_task_finish(&context->socket_session, session->session_id) != 0) {
        return context->socket_close_callback(task, EIO);
    }

    session->state = SS_OK;
    return 0;
}

static struct fast_task_info *sf_init_task(sf_socket_thread_t *context,
        int fd, const char *ip_addr, const uint16_t port,
        sf_session_entry_t *session) {

    struct fast_task_info *task;

    task = free_queue_pop();
    if (task == NULL) {
        FLOG_ERROR("malloc task buff failed, you should "
                "increase the parameter: max_connections");
        return NULL;
    }

    strcpy(task->client_ip, ip_addr);
    task->event.fd          = fd;

    task->length            = 0;
    task->offset            = 0;

    sf_task_arg_t *task_arg = task->arg;

    task_arg->session       = session;
    task_arg->context       = context;
    task_arg->response      = NULL;

    return task;
}

static struct fast_task_info *sf_clone_task(sf_socket_thread_t *context,
        const struct fast_task_info *rs) {

    struct fast_task_info *task = NULL;

    task = free_queue_pop();
    if (task == NULL) {
        FLOG_ERROR("malloc task buff failed, you should "
                "increase the parameter: max_connections");
        return NULL;
    }

    memcpy(task->client_ip, rs->client_ip, IP_ADDRESS_SIZE);
    task->event.fd          = rs->event.fd;

    task->length            = 0;
    task->offset            = 0;

    sf_task_arg_t *rs_arg   = rs->arg;
    sf_task_arg_t *task_arg = task->arg;

    task_arg->session       = rs_arg->session;
    task_arg->context       = context;
    task_arg->response      = NULL;

    return task;
}

static int sf_recv_task_done(struct fast_task_info *task) {
    request_buff_t requst_buff;

    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *session = task_arg->session;

    requst_buff.session_id = session->session_id;

    requst_buff.buff = task->data;
    requst_buff.buff_len = task->length;

    sf_socket_thread_t *context = task_arg->context;
    sf_socket_status_t *status = context->socket_status;

    context->recv_callback(&requst_buff, context->user_data);

    task->offset = 0;
    task->length = 0;

    __sync_fetch_and_add(&status->recv_pkg_count, 1);
    __sync_fetch_and_add(&status->current_recv_pkg_count, 1);

    return 0;
}

static int sf_send_task_done(struct fast_task_info *task) {
    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context = task_arg->context;
    sf_session_entry_t *session = task_arg->session;

    sf_socket_status_t *status = context->socket_status;

    if (sf_send_task_finish(&context->socket_session, session->session_id) != 0) {
        context->socket_close_callback(task, EIO);
    }

    // send queue size sub one
    __sync_fetch_and_sub(&status->current_send_queue_size, 1);
    return 0;
}

static int sf_socket_close(struct fast_task_info *task, int err) {
    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context = task_arg->context;
    sf_session_entry_t *session = task_arg->session;

    sf_socket_status_t *status = context->socket_status;

    FLOG_DEBUG("close ip:%s socket:%d session_id: %" PRId64,
            task->client_ip, task->event.fd, session->session_id);

    sf_socket_session_close(&context->socket_session, task);

    __sync_fetch_and_sub(&status->current_connections, 1);

    return 0;
    //return sf_task_finish_clean_up(task);
}

static int sf_socket_data_init(struct nio_thread_data *thread_data) {
    int result;
    sf_socket_config_t *config = &sf_config.socket_config;

    if (ioevent_init(&thread_data->ev_puller, config->max_connections + 2,
                     config->epoll_timeout, 0) != 0) {
        result = errno != 0 ? errno : ENOMEM;
        FLOG_ERROR("ioevent_init fail, "
                   "errno: %d, error info: %s",
                   result, strerror(result));
        return result;
    }

    result =
        fast_timer_init(&thread_data->timer, 2 * config->network_timeout, g_current_time);
    if (result != 0) {
        FLOG_ERROR("fast_timer_init fail, "
                   "errno: %d, error info: %s",
                   result, strerror(result));
        return result;
    }

    if (pipe(thread_data->pipe_fds) != 0) {
        result = errno != 0 ? errno : EPERM;
        FLOG_ERROR("call pipe fail, "
                   "errno: %d, error info: %s",
                   result, strerror(result));
        return result;
    }

    int fd_flags = O_NONBLOCK;
#if defined(OS_LINUX)
    fd_flags |= O_NOATIME;
#endif
    if ((result = fd_add_flags(thread_data->pipe_fds[0], fd_flags)) != 0) {
        return result;
    }

    return 0;
}

static void *accept_thread_entrance(void *arg) {
    int income_sock;
    uint16_t remote_port;
    struct sockaddr_in inaddr;
    socklen_t sockaddr_len;

    char remote_ip[IP_ADDRESS_SIZE];

    sf_socket_thread_t *context = arg;
    sf_socket_thread_config_t *config = context->socket_config;
    sf_socket_status_t *status = context->socket_status;

    while (g_continue_flag) {
        sockaddr_len = sizeof(inaddr);
        income_sock =
            accept(context->socket_fd, (struct sockaddr *)&inaddr, &sockaddr_len);

        if (income_sock < 0) {  // error
            if (!(errno == EINTR || errno == EAGAIN)) {
                FLOG_ERROR("accept failed, errno: %d, error info: %s", errno,
                           strerror(errno));
            }
            continue;
        }

        getPeerIpaddr(income_sock, remote_ip, IP_ADDRESS_SIZE);
        remote_port = ntohs(inaddr.sin_port);

        if (tcpsetnonblockopt(income_sock) != 0) {
            close(income_sock);
            continue;
        }

        int64_t session_id = __sync_add_and_fetch(&atomic_session_id, 1);


        sf_session_entry_t *session =
            sf_create_socket_session(&context->socket_session, session_id);

        session->state      = SS_OK;
        session->rtask = sf_init_task(context, income_sock, remote_ip,
                remote_port, session);

        if (session->rtask == NULL) {
            close(income_sock);
            sf_session_free(&context->socket_session, session_id);
            continue;
        }

        //set recv thread data
        session->rtask->thread_data = context->event_recv_data +
            income_sock % config->event_recv_threads;

        if (config->event_send_threads > 0) {
            session->stask = sf_clone_task(context, session->rtask);
            if (session->stask == NULL) {
                close(income_sock);
                sf_session_free(&context->socket_session, session_id);
                free_queue_push(session->rtask);
                continue;
            }

            //set send thread data
            session->stask->thread_data = context->event_send_data +
                income_sock % config->event_send_threads;
        }

        __sync_fetch_and_sub(&status->current_connections, 1);

        //not used
        //if (context->accept_done_callback != NULL) {
        //    context->accept_done_callback(read_task);
        //}

        if (session->rtask->size < config->recv_buff_size) {
            free_queue_set_buffer_size(session->rtask, config->recv_buff_size);
        }

        sf_add_recv_notify(session->rtask);
        FLOG_DEBUG("bind ip: %s, port: %d fd: %d session_id: %" PRId64,
                remote_ip, remote_port, income_sock, session_id);

    }

    __sync_fetch_and_sub(&status->actual_accept_threads, 1);

    return NULL;
}

static void *event_recv_thread_entrance(void *arg) {
    sf_socket_event_t *socket_event = arg;
    sf_socket_thread_t *context = socket_event->context;
    sf_socket_status_t *status = context->socket_status;

    struct nio_thread_data *thread_data = socket_event->thread_data;

    free(socket_event);

    ioevent_loop(thread_data, sf_notify_recv,
                 (void *)(context->socket_close_callback), &g_continue_flag);
    ioevent_destroy(&thread_data->ev_puller);

    __sync_fetch_and_sub(&status->actual_event_recv_threads, 1);
    return NULL;
}

static void *event_send_thread_entrance(void *arg) {
    sf_socket_event_t *socket_event = arg;
    sf_socket_thread_t *context = socket_event->context;
    sf_socket_status_t *status = context->socket_status;

    struct nio_thread_data *thread_data = socket_event->thread_data;

    free(socket_event);

    ioevent_loop(thread_data, sf_notify_send,
                 (void *)(context->socket_close_callback), &g_continue_flag);
    ioevent_destroy(&thread_data->ev_puller);

    __sync_fetch_and_sub(&status->actual_event_send_threads, 1);
    return NULL;
}

static int sf_accept_init(sf_socket_thread_t *context) {
    int result;

    sf_socket_thread_config_t *config = context->socket_config;
    sf_socket_status_t *status = context->socket_status;

    char name[32] = {'\0'};
    config->accept_tids = malloc(sizeof(pthread_t) * config->accept_threads);

    for (int i = 0; i < config->accept_threads; i++) {
        result = pthread_create(&config->accept_tids[i], NULL,
                accept_thread_entrance, (void *)context);

        if (result != 0) {
            FLOG_ERROR("create thread failed, startup threads: %d, "
                       "errno: %d, error info: %s",
                       i, result, strerror(result));
            return result;
        } else {
            snprintf(name, 32, "%s_accept:%d", config->thread_name_prefix, i);
            set_thread_name(config->accept_tids[i], name);

            __sync_fetch_and_add(&status->actual_accept_threads, 1);
        }
    }

    return 0;
}

static int sf_recv_init(sf_socket_thread_t *context) {
    int result;
    int bytes;

    struct nio_thread_data *it;
    struct nio_thread_data *end;

    sf_socket_thread_config_t *config = context->socket_config;
    sf_socket_status_t *status = context->socket_status;

    bytes = sizeof(struct nio_thread_data) * config->event_recv_threads;
    context->event_recv_data = (struct nio_thread_data *)malloc(bytes);

    if (context->event_recv_data == NULL) {
        FLOG_ERROR("malloc %d bytes fail, errno: %d, error info: %s", bytes, errno,
                   strerror(errno));
        return errno != 0 ? errno : ENOMEM;
    }
    memset(context->event_recv_data, 0, bytes);

    int i = 0;
    char name[32] = {'\0'};

    config->recv_tids = malloc(sizeof(pthread_t) * config->event_recv_threads);

    end = context->event_recv_data + config->event_recv_threads;
    for (it=context->event_recv_data; it<end; it++, i++) {
        // it->thread_loop_callback = thread_loop_func;
        it->arg = NULL;
        if ((result = sf_socket_data_init(it)) != 0) {
            return result;
        }

        sf_socket_event_t *socket_event = malloc(sizeof(sf_socket_event_t));
        socket_event->context = context;
        socket_event->thread_data = it;

        result = pthread_create(&config->recv_tids[i], NULL,
                event_recv_thread_entrance, socket_event);
        if (result != 0) {
            FLOG_ERROR("create thread failed, startup threads: %d, "
                       "errno: %d, error info: %s",
                       status->actual_event_recv_threads, result, strerror(result));
            break;
        } else {
            snprintf(name, 32, "%s_recv:%d", config->thread_name_prefix, i);
            set_thread_name(config->recv_tids[i], name);

            __sync_fetch_and_add(&status->actual_event_recv_threads, 1);
        }
    }

    return 0;
}

static int sf_send_init(sf_socket_thread_t *context) {
    int result;
    int bytes;

    struct nio_thread_data *it;
    struct nio_thread_data *end;

    sf_socket_thread_config_t *config = context->socket_config;
    sf_socket_status_t *status = context->socket_status;

    bytes = sizeof(struct nio_thread_data) * config->event_send_threads;
    context->event_send_data = (struct nio_thread_data *)malloc(bytes);
    if (context->event_send_data == NULL) {
        FLOG_ERROR("malloc %d bytes fail, errno: %d, error info: %s", bytes, errno,
                   strerror(errno));
        return errno != 0 ? errno : ENOMEM;
    }
    memset(context->event_send_data, 0, bytes);

    int i = 0;
    char name[32] = {'\0'};
    config->send_tids = malloc(sizeof(pthread_t) * config->event_send_threads);

    end = context->event_send_data + config->event_send_threads;
    for (it=context->event_send_data; it<end; it++, i++) {
        // it->thread_loop_callback = thread_loop_func;
        it->arg = NULL;
        if ((result = sf_socket_data_init(it)) != 0) {
            return result;
        }

        sf_socket_event_t *socket_event = malloc(sizeof(sf_socket_event_t));
        socket_event->context = context;
        socket_event->thread_data = it;

        result = pthread_create(&config->send_tids[i], NULL,
                event_send_thread_entrance, socket_event);
        if (result != 0) {
            FLOG_ERROR("create thread failed, startup threads: %d, "
                       "errno: %d, error info: %s",
                       status->actual_event_send_threads, result, strerror(result));
            break;
        } else {
            snprintf(name, 32, "%s_send:%d", config->thread_name_prefix, i);
            set_thread_name(config->send_tids[i], name);

           __sync_fetch_and_add(&status->actual_event_send_threads, 1);
        }
    }

    return 0;
}

static void sf_accept_destroy(sf_socket_thread_t *context) {
    close(context->socket_fd);

    sf_socket_thread_config_t *config = context->socket_config;

    for (int i=0; i<config->accept_threads; i++) {
        pthread_join(config->accept_tids[i], NULL);
    }
    free(config->accept_tids);

    FLOG_INFO("worker socket accep thread exit!");
}

static void sf_recv_destroy(sf_socket_thread_t *context) {
    sf_socket_thread_config_t *config = context->socket_config;

    for (int i=0; i<config->event_recv_threads; i++) {
        pthread_join(config->recv_tids[i], NULL);
    }
    free(config->recv_tids);

    struct nio_thread_data *it, *end;

    end = context->event_recv_data + config->event_recv_threads;
    for (it = context->event_recv_data; it < end; it++) {
        fast_timer_destroy(&it->timer);
    }

    free(context->event_recv_data);
    context->event_recv_data = NULL;

    FLOG_INFO("worker socket recv thread exit!");
}

static void sf_send_destroy(sf_socket_thread_t *context) {
    sf_socket_thread_config_t *config = context->socket_config;

    for (int i=0; i<config->event_send_threads; i++) {
        pthread_join(config->send_tids[i], NULL);
    }
    free(config->send_tids);

    FLOG_INFO("worker socket send thread exit!");

    struct nio_thread_data *it, *end;

    end = context->event_send_data + config->event_send_threads;
    for (it = context->event_send_data; it < end; it++) {
        fast_timer_destroy(&it->timer);
    }

    free(context->event_send_data);
    context->event_send_data = NULL;
}

void sf_socket_thread_destroy(sf_socket_thread_t *context) {
    sf_accept_destroy(context);
    sf_recv_destroy(context);
    sf_send_destroy(context);
    sf_socket_session_destroy(&context->socket_session);
}
