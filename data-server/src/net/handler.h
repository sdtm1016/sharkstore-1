_Pragma("once");

#include <functional>
#include "protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class Session;

struct Context {
    std::weak_ptr<Session> session;
    std::string remote_addr;
    std::string local_addr;
};

struct Message {
    Head head;
    std::vector<uint8_t> body;
};

using MessagePtr = std::shared_ptr<Message>;

inline MessagePtr NewMessage() { return std::make_shared<Message>(); }

using Handler = std::function<void(const Context&, const MessagePtr& msg)>;

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
