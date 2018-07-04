_Pragma("once");

#include <stdint.h>
#include <string>
#include <vector>

namespace sharkstore {
namespace raft {

enum class PeerType : char { kNormal, kLearner };

std::string PeerTypeName(PeerType type);

struct Peer {
    PeerType type = PeerType::kNormal;
    uint64_t node_id = 0;
    uint64_t peer_id = 0;

    std::string ToString() const;
};

std::string PeersToString(const std::vector<Peer>& peers);

enum class ConfChangeType : char { kAdd, kRemove, kPromote };

std::string ConfChangeTypeName(ConfChangeType type);

struct ConfChange {
    ConfChangeType type = ConfChangeType::kAdd;
    Peer peer;
    std::string context;

    std::string ToString() const;
};

} /* namespace raft */
} /* namespace sharkstore */
