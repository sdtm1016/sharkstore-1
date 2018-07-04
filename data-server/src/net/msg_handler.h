_Pragma("once");

#include "rpc_protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class Session;

struct MsgContext {
    std::weak_ptr<Session> session;
    std::string remote_addr;
    std::string local_addr;
};

using RPCHandler =
    std::function<void(const MsgContext&, const RPCHead&, std::vector<uint8_t>&&)>;

using TelnetHandler = std::function<void(const MsgContext&, std::string&& cmdline)>;

struct MsgHandler {
    RPCHandler rpc_handler;
    TelnetHandler telnet_handler;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
