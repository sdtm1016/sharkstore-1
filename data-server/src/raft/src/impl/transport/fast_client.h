_Pragma("once");

#include <atomic>
#include "base/shared_mutex.h"
#include "base/status.h"
#include "common/socket_base.h"
#include "raft/node_resolver.h"

#include "../raft_types.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class FastClient : public dataserver::common::SocketBase {
public:
    FastClient(const sf_socket_thread_config_t& cfg,
               const std::shared_ptr<NodeResolver>& resolver);
    ~FastClient();

    FastClient(const FastClient&) = delete;
    FastClient& operator=(const FastClient&) = delete;

    Status Initialize();
    void Shutdown();

    void SendMessage(MessagePtr& msg);

private:
    int64_t getSession(uint64_t to);
    void removeSession(uint64_t to);

    void send(int64_t sid, MessagePtr& msg);

private:
    sf_socket_thread_config_t config_;
    sf_socket_status_t status_;
    std::shared_ptr<NodeResolver> resolver_;

    std::atomic<int64_t> msg_id_;

    std::unordered_map<uint64_t, int64_t> sessions_;
    mutable sharkstore::shared_mutex mu_;
};

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
