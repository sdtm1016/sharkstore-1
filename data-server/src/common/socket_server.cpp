#include "socket_server.h"

#include "frame/sf_socket_session.h"
#include "frame/sf_util.h"

#include "ds_config.h"

namespace sharkstore {
namespace dataserver {
namespace common {

int SocketServer::Init(sf_socket_thread_config_t *config, sf_socket_status_t *status) {

    SocketBase::Init(config, status);

    thread_info_.is_client = false;
    thread_info_.user_data = this;

    return 0;
}

void SocketServer::set_recv_done(recv_done_callback_t recv_func) {
    thread_info_.recv_callback = recv_func;
}

void SocketServer::set_send_done(send_done_callback_t send_func) {
    thread_info_.send_callback = send_func;
}

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
