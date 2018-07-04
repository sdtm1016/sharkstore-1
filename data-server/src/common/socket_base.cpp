
#include "socket_base.h"
#include <fastcommon/shared_func.h>
#include "frame/sf_logger.h"
#include "frame/sf_socket_session.h"
#include "frame/sf_util.h"

namespace sharkstore {
namespace dataserver {
namespace common {

int SocketBase::Init(sf_socket_thread_config_t *config, sf_socket_status_t *status) {
    thread_info_.socket_config = config;
    thread_info_.socket_status = status;

    status->assigned_accept_threads = config->accept_threads;
    status->assigned_event_recv_threads = config->event_recv_threads;
    status->assigned_event_send_threads = config->event_send_threads;
    status->assigned_worker_threads = config->worker_threads;

    thread_info_.socket_fd = -1;

    thread_info_.event_recv_data = NULL;
    thread_info_.event_send_data = NULL;

    thread_info_.accept_done_callback = NULL;

    thread_info_.recv_callback = NULL;
    thread_info_.send_callback = NULL;

    thread_info_.user_data     = NULL;

    return 0;
}

int SocketBase::Start() { return sf_socket_thread_init(&thread_info_); }

void SocketBase::Stop() { sf_socket_thread_destroy(&thread_info_); }

bool SocketBase::Closed(uint64_t session_id) {
    return sf_socket_session_closed(&thread_info_.socket_session, session_id);
}

int SocketBase::Send(response_buff_t *response) {
    int result;
    // deal task queue size sub one
    __sync_fetch_and_sub(&thread_info_.socket_status->current_recv_pkg_count, 1);

    auto sid = response->session_id;
    result = sf_send_task_push(&thread_info_.socket_session, response);
    if (result != 0) {
        FLOG_ERROR("session: %" PRId64 " response fail", sid);
        return result;
    }

    // send queue size add one
    __sync_fetch_and_add(&thread_info_.socket_status->current_send_queue_size, 1);

    return 0;
}
sf_session_entry_t * SocketBase::lookup_session_entry(int64_t session_id) {
    char key[8];
    sf_session_entry_t *entry;

    long2buff(session_id, key);
    pthread_rwlock_rdlock(&thread_info_.socket_session.array_lock);
    entry = (sf_session_entry_t*)hash_find(&thread_info_.socket_session.session_array, key, 8);
    pthread_rwlock_unlock(&thread_info_.socket_session.array_lock);
    return entry;
}

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
