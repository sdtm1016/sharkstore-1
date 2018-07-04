_Pragma("once");

#include <map>
#include <vector>
#include "raft/node_resolver.h"

namespace sharkstore {
namespace raft {
namespace bench {

class NodeAddress : public NodeResolver {
public:
    NodeAddress(int n);
    ~NodeAddress();

    std::string GetNodeAddress(uint64_t node_id) override;

    uint16_t GetListenPort(uint64_t port) const;

    void GetAllNodes(std::vector<uint64_t>* nodes) const;

private:
    std::map<uint64_t, uint16_t> ports_;
};

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */
