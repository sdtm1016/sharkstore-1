#include "sf_socket_session.h"

#include <fastcommon/shared_func.h>

#include "sf_config.h"
#include "sf_logger.h"
#include "sf_socket.h"
#include "sf_util.h"
#include "sf_socket_thread.h"

static void sf_free_session_entry(sf_session_entry_t *entry);

static int socket_session_destroy(const int index, const HashData *data, void *args) {
    sf_session_entry_t *entry = (void*)data->value;
    sf_free_session_entry(entry);
    return 0;
}

int sf_socket_session_init(sf_socket_session_t *session) {
    pthread_rwlock_init(&session->array_lock, NULL);
    return hash_init(&session->session_array, simple_hash, 1024, 0.75);
}

void sf_socket_session_destroy(sf_socket_session_t *session) {

    pthread_rwlock_wrlock(&session->array_lock);
    hash_walk(&session->session_array, socket_session_destroy, NULL);
    hash_destroy(&session->session_array);
    pthread_rwlock_unlock(&session->array_lock);

    pthread_rwlock_destroy(&session->array_lock);
}

sf_session_entry_t *sf_create_socket_session(sf_socket_session_t *session,
        int64_t session_id) {

    char key[8];
    sf_session_entry_t *entry;

    long2buff(session_id, key);

    pthread_rwlock_wrlock(&session->array_lock);
    entry = hash_find(&session->session_array, key, 8);

    if (entry == NULL) {
        entry = malloc(sizeof(sf_session_entry_t));

        entry->session_id = session_id;
        entry->is_sending = false;
        entry->is_attach  = false;
        entry->send_queue = new_lk_queue();

        entry->rtask      = NULL;
        entry->stask      = NULL;

        pthread_mutex_init(&entry->swap_mutex, NULL);
        pthread_rwlock_init(&entry->session_lock, NULL);

        hash_insert(&session->session_array, key, 8, entry);
    } else {
        FLOG_ERROR("duplicated session id: %ld", session_id);
    }

    pthread_rwlock_unlock(&session->array_lock);

    return entry;
}

void sf_session_free(sf_socket_session_t *session, int64_t session_id) {
    char key[8];
    sf_session_entry_t *entry;

    long2buff(session_id, key);

    pthread_rwlock_wrlock(&session->array_lock);
    entry = hash_find(&session->session_array, key, 8);

    if (entry != NULL) {
        free(entry);
    }

    pthread_rwlock_unlock(&session->array_lock);
}

void sf_set_send_buff(struct fast_task_info *task, response_buff_t *buff) {
    assert(buff->buff != NULL);

    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *session = task_arg->session;

    pthread_mutex_lock(&session->swap_mutex);
    assert(task->length == task->offset);
    void *swap_data = task->data;

    task->data = buff->buff;        // send data buf
    task->length = buff->buff_len;  // send data length
    task->offset = 0;

    buff->buff = swap_data;  // swap task data to work task

    task_arg->response = (void *)buff;
    pthread_mutex_unlock(&session->swap_mutex);
}

static void sf_set_task_data(struct fast_task_info *task) {
    sf_task_arg_t *task_arg = task->arg;
    sf_socket_thread_t *context  = task_arg->context;
    sf_session_entry_t *session  = task_arg->session;

    pthread_mutex_lock(&session->swap_mutex);
    response_buff_t    *response = task_arg->response;
    if (response != NULL) {
        assert(response->buff != NULL);
        void *swap_data = task->data;
        task->data = response->buff;
        response->buff = swap_data;

        if (context->send_callback != NULL) {
            context->send_callback(response, context->user_data, 0);
        }
        delete_response_buff(response);
    }

    task_arg->response = NULL;

    pthread_mutex_unlock(&session->swap_mutex);
}

int sf_send_task_push(sf_socket_session_t *session, response_buff_t *buff) {
    int ret;
    char key[8];
    sf_session_entry_t *entry;

    long2buff(buff->session_id, key);

    pthread_rwlock_rdlock(&session->array_lock);
    entry = hash_find(&session->session_array, key, 8);

    do {
        if (entry == NULL) {
            FLOG_DEBUG("not found session_id: %" PRId64, buff->session_id);
            delete_response_buff(buff);
            ret = -1;
            break;
        }

        if (entry->state == SS_CLOSED) {
            FLOG_WARN("session is closed, session_id: %" PRId64, buff->session_id);
            delete_response_buff(buff);
            ret = -1;
            break;
        }

        if (entry->state == SS_INIT) {
            if (!lk_queue_push(entry->send_queue, buff)) {
                FLOG_WARN("ip: %s, session_id: %" PRId64 " send queue full",
                        entry->stask->client_ip, buff->session_id);
                delete_response_buff(buff);
                ret = -1;
            } else {
                ret = 0;
            }
            break;
        }
        pthread_rwlock_rdlock(&entry->session_lock);
        if (!__sync_bool_compare_and_swap(&entry->is_sending, false, true)) {
            if (!lk_queue_push(entry->send_queue, buff)) {
                FLOG_WARN("ip: %s, fd: %d, session_id: %" PRId64 " send queue full",
                        entry->stask->client_ip, entry->stask->event.fd, entry->session_id);

                delete_response_buff(buff);
                ret = -1;
            } else {
                FLOG_DEBUG("ip: %s, fd: %d, session_id: %" PRId64 " enqueue",
                        entry->stask->client_ip, entry->stask->event.fd, entry->session_id);
                ret = 0;
            }
            pthread_rwlock_unlock(&entry->session_lock);
            break;
            /*
            if (__sync_bool_compare_and_swap(&entry->is_sending, false, true)) {
                buff = lk_queue_pop(entry->send_queue);
                FLOG_DEBUG("ip: %s, fd: %d, session_id: %" PRId64 " not send, pop queue and send",
                        entry->stask->client_ip, entry->stask->event.fd, entry->session_id);
            } else {
                FLOG_DEBUG("ip: %s, fd: %d, session_id: %" PRId64 " enqueue",
                        entry->stask->client_ip, entry->stask->event.fd, entry->session_id);
                ret = 0;
                break;
            }
            */
        }

        FLOG_DEBUG("ip: %s, fd: %d, session_id: %" PRId64 " ready to send",
                entry->stask->client_ip, entry->stask->event.fd, entry->session_id);


        sf_set_send_buff(entry->stask, buff);

        ret = sf_add_send_notify(entry->stask);

        pthread_rwlock_unlock(&entry->session_lock);

        if (ret != 0) {
            FLOG_ERROR("ip: %s, fd: %d, session_id: %" PRId64 " add send event fail",
                    entry->stask->client_ip, entry->stask->event.fd, entry->session_id);
            break;
        }

        ret = 0;
    } while (false);

    pthread_rwlock_unlock(&session->array_lock);

    return ret;
}

int sf_send_task_finish(sf_socket_session_t *session, int64_t session_id) {
    int ret;
    char key[8];

    response_buff_t *buff;
    sf_session_entry_t *entry;

    long2buff(session_id, key);
    pthread_rwlock_rdlock(&session->array_lock);
    entry = hash_find(&session->session_array, key, 8);

    do {
        if (entry == NULL) {
            FLOG_WARN("session_id: %" PRId64 " not found", entry->session_id);
            ret = 0;
            break;
        }

        FLOG_DEBUG("session_id: %" PRId64 " send finish", entry->session_id);

        //swap send buff
        sf_set_task_data(entry->stask);

        buff = lk_queue_pop(entry->send_queue);
        if (buff == NULL) {

            pthread_rwlock_wrlock(&entry->session_lock);
            buff = lk_queue_pop(entry->send_queue);

            if (buff == NULL) {
                if ((ret = sf_clear_send_event(entry->stask)) != 0) {
                    FLOG_ERROR("ip: %s, fd: %d, clear send event fail, session: %" PRId64 ,
                            entry->stask->client_ip, entry->stask->event.fd, session_id);
                }
                assert(entry->is_sending);
                __sync_bool_compare_and_swap(&entry->is_sending, true, false);
                pthread_rwlock_unlock(&entry->session_lock);
                break;
            }

            pthread_rwlock_unlock(&entry->session_lock);
        }

        FLOG_DEBUG("ip: %s, fd: %d, session_id: %" PRId64 " ready to send",
                entry->stask->client_ip, entry->stask->event.fd, entry->session_id);

        sf_set_send_buff(entry->stask, buff);

        if (entry->is_attach) {
            ret = sf_set_request_timeout(entry->stask);
        } else {
            //todo ? why call this
            //ret = sf_add_send_notify(entry->stask);
            if(__sync_bool_compare_and_swap(&entry->is_attach, false, true)) {
                ret = sf_set_send_event(entry->stask);
            } else {
                ret = 0;
            }
        }
    } while (false);

    pthread_rwlock_unlock(&session->array_lock);

    return ret;
}

void sf_socket_session_close(sf_socket_session_t *session,
        struct fast_task_info *task) {

    char key[8];
    sf_session_entry_t *entry;

    sf_task_arg_t *task_arg = task->arg;
    sf_session_entry_t *se = task_arg->session;

    struct fast_task_info *close_task = NULL;

    long2buff(se->session_id, key);
    pthread_rwlock_wrlock(&session->array_lock);
    entry = hash_find(&session->session_array, key, 8);

    int fd = task->event.fd;

    if (entry != NULL) {

        FLOG_DEBUG("ip: %s, fd: %d, session_id: %" PRId64
                " read task:%p send task:%p socket state: %d",
                task->client_ip, fd, entry->session_id,
                entry->rtask, entry->stask, entry->state);

        entry->state = SS_CLOSED;

        if (entry->rtask == task) {
            sf_clear_recv_event(task);
            free_queue_push(task); //recycle task
            entry->rtask = NULL;

            if (entry->stask != NULL) {
                if (__sync_bool_compare_and_swap(&entry->is_sending, false, true)) {
                    if (sf_add_send_notify(entry->stask) != 0) {
                        FLOG_ERROR("ip: %s, fd: %d, session_id: %" PRId64
                                " add write event to send task fail",
                                task->client_ip, fd, entry->session_id);
                    }
                }
            }
        }

        if (entry->stask == task) {
            sf_set_task_data(task);

            sf_clear_send_event(task);
            free_queue_push(task); //recycle task
            entry->stask = NULL;

            close_task = entry->rtask;
        }

        if (close_task != NULL) {
            sf_set_close_event(close_task);
        }

        if (entry->rtask == NULL && entry->stask == NULL) {
            FLOG_DEBUG("real close session_id: %" PRId64 " fd: %d socket state: %d",
                    entry->session_id, fd, entry->state);

            //close socket fd
            if (fd != -1) {
                close(fd);
            }

            hash_delete(&session->session_array, key, 8);

            sf_free_session_entry(entry);
        }
    }
    pthread_rwlock_unlock(&session->array_lock);
}

bool sf_socket_session_closed(sf_socket_session_t *session, int64_t session_id) {
    char key[8];
    bool is_closed = true;

    sf_session_entry_t *entry;

    long2buff(session_id, key);
    pthread_rwlock_rdlock(&session->array_lock);
    entry = hash_find(&session->session_array, key, 8);
    if (entry != NULL) {
        if (entry->state != SS_CLOSED) {
            is_closed = false;
        }
    }
    pthread_rwlock_unlock(&session->array_lock);

    return is_closed;
}

void sf_free_session_entry(sf_session_entry_t *entry) {
    response_buff_t *buff = lk_queue_pop(entry->send_queue);
    while (buff != NULL) {
        //callback?
        delete_response_buff(buff);
        buff = lk_queue_pop(entry->send_queue);
    }

    delete_lk_queue(entry->send_queue);
    pthread_rwlock_destroy(&entry->session_lock);
    pthread_mutex_destroy(&entry->swap_mutex);
    free(entry);
}
