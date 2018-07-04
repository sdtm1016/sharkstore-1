_Pragma("once");

#include "transport.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class FastConnection : public Connection {
public:
    FastConnection() = default;
    ~FastConnection();

    Status Open(const std::string& ip, uint16_t port);
    Status Send(MessagePtr& msg) override;
    Status Close() override;

private:
    int sockfd_ = -1;
};

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
