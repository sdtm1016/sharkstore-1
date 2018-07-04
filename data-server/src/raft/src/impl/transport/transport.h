_Pragma("once");

#include <functional>
#include "base/status.h"
#include "../raft_types.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

// 处理收到的消息
typedef std::function<void(MessagePtr&)> MessageHandler;

class Connection {
public:
    Connection() = default;
    virtual ~Connection() = default;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    virtual Status Send(MessagePtr& msg) = 0;

    virtual Status Close() = 0;
};

class Transport {
public:
    Transport() = default;
    virtual ~Transport() = default;

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;

    virtual Status Start(const std::string& listen_ip, uint16_t listen_port,
                         const MessageHandler& handler) = 0;
    virtual void Shutdown() = 0;

    virtual void SendMessage(MessagePtr& msg) = 0;

    // 需要单独建立一个连接用来发快照
    virtual Status GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) = 0;
};

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
