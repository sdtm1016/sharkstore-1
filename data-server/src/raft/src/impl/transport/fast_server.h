_Pragma("once");

#include "common/socket_base.h"
#include "transport.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class FastServer : public dataserver::common::SocketBase {
public:
    FastServer(const sf_socket_thread_config_t& config,
               const MessageHandler& handler);
    ~FastServer();

    FastServer(const FastServer&) = delete;
    FastServer& operator=(const FastServer&) = delete;

    Status Initialize();
    void Shutdown();

private:
    friend void fastserver_recv_task_cb(request_buff_t*, void*);
    friend void fastserver_send_done_cb(response_buff_t*, void*, int);

    void handleTask(request_buff_t* task);
    void sendDoneCallback(response_buff_t* task, int err);

private:
    sf_socket_thread_config_t config_;
    sf_socket_status_t status_;
    MessageHandler handler_;
};

} /* namespace transport */
}  // namespace impl
}  // namespace raft
}  // namespace sharkstore
