_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "raft/raft.h"
#include "raft/server.h"
#include "range.h"

namespace sharkstore {
namespace raft {
namespace bench {

class NodeAddress;

class Node {
public:
    Node(uint64_t node_id, std::shared_ptr<NodeAddress> addrs);
    ~Node();

    void Start();
    std::shared_ptr<Range> GetRange(uint64_t i);

private:
    const uint64_t node_id_;
    std::shared_ptr<NodeAddress> addr_mgr_;

    std::unique_ptr<RaftServer> raft_server_;
    std::unordered_map<uint64_t, std::shared_ptr<Range>> ranges_;
};

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */
