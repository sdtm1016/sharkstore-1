#include "address.h"

namespace sharkstore {
namespace raft {
namespace bench {

NodeAddress::NodeAddress(int n) {
    if (n < 1 && n > 9) {
        throw std::runtime_error("invalid node num, must be 1 to 9");
    }
    for (uint16_t i = 1; i <= n; ++i) {
        ports_.emplace(i, 9990 + i);
    }
}

NodeAddress::~NodeAddress() {}

std::string NodeAddress::GetNodeAddress(uint64_t node_id) {
    return std::string("127.0.0.1:") + std::to_string(GetListenPort(node_id));
}

uint16_t NodeAddress::GetListenPort(uint64_t node_id) const {
    auto it = ports_.find(node_id);
    if (it == ports_.cend()) {
        throw std::runtime_error("invalid node id, must be 1 to 9");
    } else {
        return it->second;
    }
}

void NodeAddress::GetAllNodes(std::vector<uint64_t>* nodes) const {
    for (auto it = ports_.cbegin(); it != ports_.cend(); ++it) {
        nodes->push_back(it->first);
    }
}

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */