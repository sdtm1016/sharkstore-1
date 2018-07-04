#ifndef __SOCKET_BASE_H__
#define __SOCKET_BASE_H__

#include "frame/sf_config.h"
#include "frame/sf_socket_thread.h"
#include "frame/sf_status.h"

namespace sharkstore {
namespace dataserver {
namespace common {

class SocketBase {
public:
    SocketBase() = default;
    virtual ~SocketBase() = default;

    SocketBase(const SocketBase &) = delete;
    SocketBase &operator=(const SocketBase &) = delete;
    SocketBase &operator=(const SocketBase &) volatile = delete;

    virtual int Init(sf_socket_thread_config_t *config, sf_socket_status_t *status);
    virtual int Start();
    virtual void Stop();

    virtual int Send(response_buff_t *response);
    virtual bool Closed(uint64_t session_id);
    sf_session_entry_t* lookup_session_entry(int64_t session_id);

protected:
    sf_socket_thread_t thread_info_;
};

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
#endif  //__SOCKET_BASE_H__
