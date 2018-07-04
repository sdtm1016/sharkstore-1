_Pragma("once");

#include <iostream>
#include <map>
#include "raft/options.h"

namespace sharkstore {
namespace raft {
namespace playground {

static const uint64_t kMaxNodeID = 9;

class AddressManager : public NodeResolver {
public:
    AddressManager() {
        for (uint64_t i = 1; i <= kMaxNodeID; ++i) {
            raft_ports_[i] = 9980 + i;
            tel_ports_[i] = 9990 + i;
        }
    }

    std::string GetNodeAddress(uint64_t node_id) override {
        auto it = raft_ports_.find(node_id);
        if (it == raft_ports_.end()) {
            return "";
        } else {
            return std::string("127.0.0.1:") + std::to_string(it->second);
        }
    }

    uint16_t GetRaftPort(uint16_t node_id) {
        auto it = raft_ports_.find(node_id);
        if (it == raft_ports_.end()) {
            return 0;
        } else {
            return it->second;
        }
    }

    uint16_t GetTelnetPort(uint64_t node_id) {
        auto it = tel_ports_.find(node_id);
        if (it == tel_ports_.end()) {
            return 0;
        } else {
            return it->second;
        }
    }

private:
    std::map<uint64_t, uint16_t> raft_ports_;
    std::map<uint64_t, uint16_t> tel_ports_;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */
