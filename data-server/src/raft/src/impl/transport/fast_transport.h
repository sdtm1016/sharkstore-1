_Pragma("once");

#include "common/socket_server.h"
#include "raft/node_resolver.h"

#include "transport.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class FastServer;
class FastClient;

class FastTransport : public Transport {
public:
    FastTransport(const std::shared_ptr<NodeResolver>& resolver,
                  size_t send_threads_num, size_t recv_threads_num);
    ~FastTransport();

    Status Start(const std::string& listen_ip, uint16_t listen_port,
                 const MessageHandler& handler) override;

    void Shutdown() override;

    void SendMessage(MessagePtr& msg) override;

    Status GetConnection(uint64_t to,
                         std::shared_ptr<Connection>* conn) override;

private:
    std::shared_ptr<NodeResolver> resolver_;
    const size_t recv_threads_num_ = 0;

    FastServer* server_ = nullptr;
    FastClient* client_ = nullptr;
};

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
