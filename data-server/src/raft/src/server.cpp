#include "raft/server.h"
#include "impl/server_impl.h"

namespace sharkstore {
namespace raft {

std::unique_ptr<RaftServer> CreateRaftServer(const RaftServerOptions& ops) {
    return std::unique_ptr<RaftServer>(new impl::RaftServerImpl(ops));
}

} /* namespace raft */
} /* namespace sharkstore */