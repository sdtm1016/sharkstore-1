#include "sf_socket.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <fastcommon/pthread_func.h>
#include <fastcommon/sched_thread.h>
#include <fastcommon/shared_func.h>
#include <fastcommon/sockopt.h>

#include "sf_config.h"
#include "sf_logger.h"
#include "sf_socket_thread.h"
#include "sf_socket_session.h"

static int sf_proto_header_size = 0;

static sf_body_length_callback_t sf_body_length_callback = NULL;

static void sf_event_connect(int sock, short event, void *arg);
static void sf_event_close(int sock, short event, void *arg);
static void sf_event_recv(int sock, short event, void *arg);
static void sf_event_send(int sock, short event, void *arg);

static void sf_socket_notify(int sock, short event, void *arg, int type);

void sf_set_header_size(int size) { sf_proto_header_size = size; }

void sf_set_body_length_callback(sf_body_length_callback_t body_length_func) {
    sf_body_length_callback = body_length_func;
}

static int sf_set_add_event(struct fast_task_info *task) {
    long task_ptr = (long)task;
    size_t len = sizeof(task_ptr);

    if (write(task->thread_data->pipe_fds[1], &task_ptr, len) != len) {
        FLOG_ERROR("call write to pipe fd: %d fail, "
                   "errno: %d, error info: %s",
                   task->thread_data->pipe_fds[1], errno, strerror(errno));

        return -1;
    }

    return 0;
}

int sf_add_connect_notify(struct fast_task_info *task) {
    return sf_add_send_notify(task);
}

int sf_add_recv_notify(struct fast_task_info *task) {
    return sf_set_add_event(task);
}

int sf_add_send_notify(struct fast_task_info *task) {
    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *session = task_arg->session;

    //set callback for send buf
    task->event.callback = sf_event_send;

    if (__sync_bool_compare_and_swap(&session->is_attach, false, true)) {

        return sf_set_add_event(task);
    }

    return 0;
}

void sf_set_send_callback(struct fast_task_info *task) {
    //set callback for send buf
    task->event.callback = sf_event_send;
}

//sf_config.socket_config.socket_keep_time
int sf_set_socket_keep(struct fast_task_info *task) {
    int keep_time = sf_config.socket_config.socket_keep_time;

    return fast_timer_modify(&task->thread_data->timer, &task->event.timer,
                      g_current_time + keep_time);
}

int sf_set_request_timeout(struct fast_task_info *task) {
    int timeout = sf_config.socket_config.network_timeout;

    return fast_timer_modify(&task->thread_data->timer, &task->event.timer,
                      g_current_time + timeout);
}

int sf_set_connect_event(struct fast_task_info *task) {
    int timeout = sf_config.socket_config.connect_timeout;

    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *session = task_arg->session;

    if (ioevent_set(task, task->thread_data, task->event.fd, IOEVENT_WRITE,
            sf_event_connect, timeout) != 0) {
        FLOG_ERROR("client ip: %s, fd: %d session: %" PRId64
                " set connect event fail",
                task->client_ip, task->event.fd, session->session_id);

        __sync_bool_compare_and_swap(&session->is_attach, true, false);
        return -1;
    }

    return 0;
}

int sf_set_close_event(struct fast_task_info *task) {
    task->event.callback = sf_event_close;
    return 0;
}

int sf_set_recv_event(struct fast_task_info *task) {
    int timeout = sf_config.socket_config.socket_keep_time;

    return ioevent_set(task, task->thread_data, task->event.fd, IOEVENT_READ,
                sf_event_recv, timeout);
}

int sf_set_send_event(struct fast_task_info *task) {
    int timeout = sf_config.socket_config.network_timeout;

    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *session = task_arg->session;

    if (ioevent_set(task, task->thread_data, task->event.fd,
                IOEVENT_WRITE, sf_event_send, timeout) != 0) {

        __sync_bool_compare_and_swap(&session->is_attach, true, false);
        return -1;
    }

    return 0;
}

static void sf_event_connect(int sock, short event, void *arg) {
    struct fast_task_info *task;

    assert(sock >= 0);
    task = (struct fast_task_info *)arg;

    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context = task_arg->context;
    sf_session_entry_t *session = task_arg->session;

    sf_socket_status_t *status = context->socket_status;

    assert(session->state == SS_INIT);
    if (session->state != SS_INIT) {
        FLOG_WARN("client ip: %s, fd: %d connect state error, state: %d",
                task->client_ip, sock, session->state);
        context->socket_close_callback(task, EIO);
        return;
    }

    if (event & IOEVENT_TIMEOUT) {
        FLOG_ERROR("client ip: %s, fd: %d connect timeout.",
                task->client_ip, sock);

        __sync_fetch_and_add(&status->connect_timeout_count, 1);
        context->socket_close_callback(task, ETIME);
        return;
    }

    if (event & IOEVENT_ERROR) {
        FLOG_DEBUG("client ip: %s, fd: %d connect error."
                " errno: %d, err: %s",
                task->client_ip, sock, errno, strerror(errno));

        __sync_fetch_and_add(&status->connect_error_count, 1);
        context->socket_close_callback(task, EIO);
        return;
    }

    //client connect
    if (context->enable_done_callback != NULL) {
        context->enable_done_callback(task);
    }
}

static void sf_event_close(int sock, short event, void *arg) {
    struct fast_task_info *task;

    assert(sock >= 0);
    task = (struct fast_task_info *)arg;
    sf_task_arg_t *task_arg = task->arg;

    sf_socket_thread_t *context = task_arg->context;
    sf_session_entry_t *session = task_arg->session;

    if (session->state != SS_CLOSED) {
        FLOG_WARN("client ip: %s, fd: %d connect state error, state: %d",
                task->client_ip, sock, session->state);
    }

    FLOG_WARN("client ip: %s, fd: %d ready to close, state: %d",
            task->client_ip, sock, session->state);

    context->socket_close_callback(task, EIO);
}

static void sf_event_recv(int sock, short event, void *arg) {
    int bytes;
    int recv_bytes;
    struct fast_task_info *task;

    assert(sock >= 0);
    task = (struct fast_task_info *)arg;
    sf_task_arg_t *task_arg = task->arg;

    sf_socket_thread_t *context = task_arg->context;
    sf_socket_status_t *status = context->socket_status;

    if (event & IOEVENT_TIMEOUT) {
        __sync_fetch_and_add(&status->recv_timeout_count, 1);

        if (task->length > 0) {
            FLOG_ERROR("client ip: %s, fd: %d recv timeout, "
                    "recv offset: %d, expect length: %d",
                    task->client_ip, sock, task->offset, task->length);
        } else {
            FLOG_WARN("client ip: %s, fd: %d recv timeout",
                    task->client_ip, sock);
        }

        context->socket_close_callback(task, ETIME);
        return;
    }

    if (event & IOEVENT_ERROR) {
        FLOG_DEBUG("client ip: %s, fd: %d recv error."
                " errno: %d, err: %s",
                task->client_ip, sock, errno, strerror(errno));

        __sync_fetch_and_add(&status->recv_error_count, 1);
        context->socket_close_callback(task, EIO);
        return;
    }

    while (true) {

        if (task->length == 0) {  // recv header
            recv_bytes = sf_proto_header_size - task->offset;
        } else {
            recv_bytes = task->length - task->offset;
        }

        bytes = read(sock, task->data + task->offset, recv_bytes);
        int err = errno;
        if (bytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                //sf_set_socket_keep(task);

                FLOG_DEBUG("client ip: %s, fd: %d read signal again",
                        task->client_ip, sock);
                break;
            } else if (errno == EINTR) {  // should retry
                FLOG_DEBUG("client ip: %s, fd: %d read ignore interupt signal",
                        task->client_ip, sock);
                continue;
            } else {
                FLOG_WARN("client ip: %s, fd: %d recv failed, "
                          "errno: %d, error info: %s",
                          task->client_ip, sock, err, strerror(err));

                __sync_fetch_and_add(&status->recv_error_count, 1);
                context->socket_close_callback(task, err);
                return;
            }
        } else if (bytes == 0) {
            FLOG_WARN("client ip: %s, sock: %d, recv failed, "
                       "connection disconnected",
                       task->client_ip, sock);

            __sync_fetch_and_add(&status->recv_error_count, 1);
            context->socket_close_callback(task, err);
            return ;
        }

        task->offset += bytes;

        if (bytes < recv_bytes) {
            FLOG_DEBUG("client ip: %s, fd: %d body: %d, need recv: %d, recv: %d",
                    task->client_ip, sock, task->length, recv_bytes, bytes);
            break;
        }

        if (task->length == 0) {  // proto header
            //set task->length value
            if (sf_body_length_callback(task) != 0) {
                FLOG_ERROR("client ip: %s, fd: %d set task length error",
                        task->client_ip, sock);

                __sync_fetch_and_add(&status->recv_error_count, 1);
                context->socket_close_callback(task, 0);
                return;
            }

            if (task->length < 0) {
                FLOG_ERROR("client ip: %s, fd: %d pkg length: %d < 0",
                        task->client_ip, sock, task->length);

                __sync_fetch_and_add(&status->recv_error_count, 1);
                context->socket_close_callback(task, 0);
                return;
            }

            task->length += sf_proto_header_size;
            if (task->length > sf_config.socket_config.max_pkg_size) {
                FLOG_ERROR("client ip: %s, fd: %d, pkg length: %d > "
                           "max pkg size: %d",
                           task->client_ip, sock, task->length,
                           sf_config.socket_config.max_pkg_size);

                __sync_fetch_and_add(&status->big_len_pkg_count, 1);
                context->socket_close_callback(task, 0);
                return;
            }

            if (task->length > task->size) {
                int old_size;
                old_size = task->size;

                FLOG_WARN("client ip: %s, fd: %d task length: %d realloc buffer size "
                           "from %d to %d",
                           task->client_ip, sock, task->length, old_size, task->length);

                if (free_queue_realloc_buffer(task, task->length) != 0) {
                    FLOG_ERROR("client ip: %s, fd: %d realloc buffer size "
                               "from %d to %d fail",
                               task->client_ip, sock, task->size, task->length);

                   context->socket_close_callback(task, ENOMEM);
                   return;
                }
            }
        }

        if (task->offset >= task->length) {  // recv done
            sf_set_socket_keep(task);
            context->recv_done_callback(task);
            break;
        }
    }
}

static void sf_event_send(int sock, short event, void *arg) {
    int bytes;
    int send_bytes;
    struct fast_task_info *task;

    assert(sock >= 0);
    task = (struct fast_task_info *)arg;

    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context = task_arg->context;
    sf_session_entry_t *session = task_arg->session;

    sf_socket_status_t *status = context->socket_status;

    if (event & IOEVENT_TIMEOUT) {
        FLOG_ERROR("client ip: %s, fd: %d, session: %" PRId64
                " send timeout. total length: %d,"
                " offset: %d, remain: %d",
                task->client_ip, sock, session->session_id,
                task->length, task->offset,
                task->length - task->offset);

        __sync_fetch_and_add(&status->send_timeout_count, 1);
        context->socket_close_callback(task, ETIME);
        return;
    }

    if (event & IOEVENT_ERROR) {
        FLOG_WARN("client ip: %s, fd: %d, session: %" PRId64
                " send error. errno: %d, err: %s",
                task->client_ip, sock, session->session_id,
                errno, strerror(errno));

        __sync_fetch_and_add(&status->send_error_count, 1);
        context->socket_close_callback(task, EIO);
        return;
    }

    while (true) {
        send_bytes = task->length - task->offset;

        FLOG_DEBUG("client ip: %s, fd: %d, session: %" PRId64
                " ready to totle_bytes: %d send_bytes: %d",
                task->client_ip, sock, session->session_id, task->length, send_bytes);

        bytes = write(sock, task->data + task->offset, send_bytes);
        if (bytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                FLOG_DEBUG("client ip: %s, fd: %d,  session: %" PRId64
                        " send again signal",
                        task->client_ip, sock, session->session_id);
                break;
            } else if (errno == EINTR) {  // should retry
                FLOG_DEBUG("client ip: %s, fd: %d, session: %" PRId64
                        " send ignore interupt signal",
                        task->client_ip, sock, session->session_id);
                continue;
            } else {
                FLOG_WARN("client ip: %s, fd: %d, session: %" PRId64
                        " send fail, errno: %d, error info: %s",
                          task->client_ip, sock, session->session_id,
                          errno, strerror(errno));

                __sync_fetch_and_add(&status->send_error_count, 1);
               context->socket_close_callback(task, EIO);
               return;
            }
        }

        task->offset += bytes;

        if (bytes < send_bytes) {
            FLOG_INFO("client ip: %s, fd: %d, session:%" PRId64
                    " body: %d, need send: %d, send: %d",
                    task->client_ip, sock, session->session_id,
                    task->length, send_bytes, bytes);
            break;
        }

        // send end and recycle task
        if (task->offset >= task->length) {
            FLOG_DEBUG("client ip: %s, fd: %d, session: %" PRId64
                      " send end. totle_bytes: %d ",
                       task->client_ip, sock, session->session_id, task->length);
            task->length = 0;
            task->offset = 0;
            context->send_done_callback(task);
            break;
        }
    }
}

int sf_socket_send_task(struct fast_task_info *task) {
    int bytes;
    int send_bytes;
    int err;
    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context = task_arg->context;
    sf_socket_status_t *status = context->socket_status;
    sf_session_entry_t *session = task_arg->session;

    FLOG_DEBUG("client ip: %s, fd: %d, session_id: %" PRId64
            " direct send begin",
            task->client_ip, task->event.fd, session->session_id);

    while (true) {
        send_bytes = task->length - task->offset;
        bytes = write(task->event.fd, task->data + task->offset, send_bytes);
        err = errno;

        if (bytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                FLOG_DEBUG("client ip: %s, fd: %d session: %" PRId64
                        " send again signal",
                        task->client_ip, task->event.fd, session->session_id);

                if (sf_add_send_notify(task) != 0) {
                    FLOG_ERROR("client ip: %s, fd: %d, session: %" PRId64
                            " set send event fail",
                            task->client_ip, task->event.fd, session->session_id);

                    __sync_fetch_and_add(&status->send_error_count, 1);
                    context->socket_close_callback(task, err);
                    return -1;
                }

                break;
            } else if (errno == EINTR) {  // should retry
                FLOG_DEBUG("client ip: %s, fd: %d, session: %" PRId64
                        " send ignore interupt signal",
                        task->client_ip, task->event.fd, session->session_id);
                continue;
            } else {
                FLOG_ERROR("client ip: %s, fd: %d session: %" PRId64
                        " send fail, errno: %d, error info: %s",
                          task->client_ip, task->event.fd, session->session_id,
                          errno, strerror(errno));

                __sync_fetch_and_add(&status->send_error_count, 1);
                context->socket_close_callback(task, err);
                return -1;
            }
        }

        task->offset += bytes;

        if (bytes < send_bytes) {
            FLOG_WARN("client ip: %s, fd: %d need send_bytes: %d, send_bytes: %d",
                    task->client_ip, task->event.fd, send_bytes, bytes);

            if (sf_add_send_notify(task) != 0) {
                FLOG_ERROR("client ip: %s, fd: %d set send event fail",
                        task->client_ip, task->event.fd);

                context->socket_close_callback(task, EBADF);
                return -1;
            }
            break;
        }

        // send end and recycle task
        if (task->offset >= task->length) {
            task->length = 0;
            task->offset = 0;
            context->send_done_callback(task);
            break;
        }
    }

    return 0;
}

int sf_socket_server(const char *bind_addr, int port, int *sock) {
    int result;
    *sock = socketServer(bind_addr, port, &result);
    if (*sock < 0) {
        return result;
    }

    if ((result = tcpsetserveropt(*sock, sf_config.socket_config.network_timeout)) != 0) {
        return result;
    }
    return 0;
}

int sf_connect_to_server(const char *host_addr, const uint16_t port, int *sock) {
    int result;
    struct sockaddr_in addr;
    struct sockaddr_in6 addr6;
    void *dest;
    int size;
    char ip_addr[IP_ADDRESS_SIZE];

    if (getIpaddrByName(host_addr, ip_addr, sizeof(ip_addr)) == INADDR_NONE) {
        FLOG_ERROR("resolve domain \"%s\" fail", host_addr);
        return EINVAL;
    }

    FLOG_DEBUG("host_addr:\"%s\" => ip_addr: \"%s\"", host_addr, ip_addr);

    int  fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return errno != 0 ? errno : ENOMEM;
    }

    do {
        if ((result = tcpsetserveropt(fd,
                        sf_config.socket_config.network_timeout)) != 0) {
            break;
        }

        if ((result = tcpsetnonblockopt(fd)) != 0) {
            break;
        }

        if ((result=setsockaddrbyip(ip_addr, port, &addr, &addr6,
                        &dest, &size)) != 0) {
            break;
        }

        if ((result = connect(fd, (const struct sockaddr*)dest, size)) < 0) {
            if (errno != EINPROGRESS) {
                break;
            }
        }

        *sock = fd;
        return 0;
    } while (false);

    close(fd);
    *sock = -1;
    return result;
}

int sf_clear_recv_event(struct fast_task_info *task) {
    int ret = 0;
    if (task->finish_callback != NULL) {
        task->finish_callback(task);
        task->finish_callback = NULL;
    }

    ret = ioevent_detach(&task->thread_data->ev_puller, task->event.fd);
    if (ret != 0) {
        FLOG_WARN("ioevent_detach: socket: %d, errno: %d  err:%s",
                task->event.fd, ret, strerror(ret));
    }

    if (task->event.timer.expires > 0) {
        ret = fast_timer_remove(&task->thread_data->timer, &task->event.timer);
        if (ret) {
            FLOG_WARN("timer remove: socket: %d, ret: %d  err:%s",
                    task->event.fd, ret, strerror(ret));
        }

        task->event.timer.expires = 0;
    }

    ret = ioevent_remove(&task->thread_data->ev_puller, task);
    if (ret) {
        FLOG_WARN("ioevent_remove: socket: %d, ret: %d  err:%s",
                task->event.fd, ret, strerror(ret));
    }

    return ret;
}

int sf_clear_send_event(struct fast_task_info *task) {
    int ret = 0;
    if (task->finish_callback != NULL) {
        task->finish_callback(task);
        task->finish_callback = NULL;
    }

    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *session = task_arg->session;

    if (__sync_bool_compare_and_swap(&session->is_attach, true, false)) {
        ret = ioevent_detach(&task->thread_data->ev_puller, task->event.fd);

        if (ret != 0 && errno != ENOENT) {
            FLOG_ERROR("ioevent_detach: socket: %d, errno: %d  err:%s",
                    task->event.fd, errno, strerror(errno));
        } else {
            ret = 0;
        }

        if (task->event.timer.expires > 0) {
            ret = fast_timer_remove(&task->thread_data->timer, &task->event.timer);
            if (ret) {
                FLOG_WARN("timer remove: socket: %d, ret: %d  err:%s",
                        task->event.fd, ret, strerror(ret));
            }

            task->event.timer.expires = 0;
        }

        ret = ioevent_remove(&task->thread_data->ev_puller, task);
        if (ret) {
            FLOG_WARN("ioevent_remove: socket: %d, ret: %d  err:%s",
                    task->event.fd, ret, strerror(ret));
        }
    }

    return ret;
}

void sf_notify_recv(int sock, short event, void *arg) {
    sf_socket_notify(sock, event, arg, IOEVENT_READ);
}

void sf_notify_send(int sock, short event, void *arg) {
    sf_socket_notify(sock, event, arg, IOEVENT_WRITE);
}

void sf_socket_notify(int sock, short event, void *arg, int type) {
    int bytes;
    long task_ptr;
    struct fast_task_info *task;

    while (true) {
        if ((bytes = read(sock, &task_ptr, sizeof(task_ptr))) < 0) {
            if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
                FLOG_ERROR("call read failed, errno: %d, error info: %s", errno,
                           strerror(errno));
            }
            break;
        } else if (bytes == 0) {
            break;
        }

        task = (struct fast_task_info *)task_ptr;
        sf_task_arg_t *task_arg = task->arg;
        sf_socket_thread_t *context = task_arg->context;
        sf_session_entry_t *session = task_arg->session;

        if (type == IOEVENT_READ) {
            if (sf_set_recv_event(task) != 0) {
                FLOG_WARN("client ip: %s, fd: %d, session: %" PRId64
                        " add recv event error",
                        task->client_ip, sock, session->session_id);

                context->socket_close_callback(task, EIO);
            }

            continue;
        }

        //type == IOEVENT_WRITE
        if (session->state == SS_INIT) {
            if (sf_set_connect_event(task) != 0) {
                FLOG_WARN("client ip: %s, fd: %d add connect event error",
                        task->client_ip, sock);

                context->socket_close_callback(task, EIO);
            }
        } else {
            if (sf_set_send_event(task) != 0) {
                FLOG_WARN("client ip: %s, fd: %d add recv event error",
                        task->client_ip, sock);

                context->socket_close_callback(task, EIO);
            }
        }
    }
}
