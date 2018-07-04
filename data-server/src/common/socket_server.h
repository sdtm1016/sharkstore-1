#ifndef FBASE_DATASERVER_COMMON_SOCKET_SERVER_H_
#define FBASE_DATASERVER_COMMON_SOCKET_SERVER_H_

#include "frame/sf_config.h"
#include "frame/sf_socket_thread.h"
#include "frame/sf_status.h"

#include "socket_base.h"

namespace sharkstore {
namespace dataserver {
namespace common {

class SocketServer : virtual public SocketBase {
public:
    SocketServer() = default;
    ~SocketServer() = default;

    SocketServer(const SocketServer &) = delete;
    SocketServer &operator=(const SocketServer &) = delete;
    SocketServer &operator=(const SocketServer &) volatile = delete;

    virtual int Init(sf_socket_thread_config_t *config, sf_socket_status_t *status);

    void set_recv_done(recv_done_callback_t recv_func);
    void set_send_done(send_done_callback_t send_func);
};

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
#endif  // FBASE_DATASERVER_COMMON_SOCKET_SERVER_H_
