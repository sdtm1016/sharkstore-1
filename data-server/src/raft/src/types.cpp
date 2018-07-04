#include "raft/types.h"

#include <sstream>

namespace sharkstore {
namespace raft {

std::string PeerTypeName(PeerType type) {
    switch (type) {
        case PeerType::kNormal:
            return "normal";
        case PeerType::kLearner:
            return "learner";
        default:
            return std::string("unknown(") + std::to_string(static_cast<int>(type)) + ")";
    }
}

std::string Peer::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"type\": \"" << PeerTypeName(type) << "\", ";
    ss << "\"node_id\": " << node_id << ", ";
    ss << "\"peer_id\": " << peer_id;
    ss << "}";
    return ss.str();
}

std::string PeersToString(const std::vector<Peer>& peers) {
    std::string ret = "[";
    for (size_t i = 0; i < peers.size(); ++i) {
        ret += peers[i].ToString();
        if (i != peers.size() - 1) {
            ret += ", ";
        }
    }
    ret += "]";
    return ret;
}

std::string ConfChangeTypeName(ConfChangeType type) {
    switch (type) {
        case ConfChangeType::kAdd:
            return "add";
        case ConfChangeType::kRemove:
            return "remove";
        case ConfChangeType::kPromote:
            return "promote";
        default:
            return std::string("unknown(") + std::to_string(static_cast<int>(type)) + ")";
    }
}

std::string ConfChange::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"type\": \"" << ConfChangeTypeName(type) << "\", ";
    ss << "\"peer\": " << peer.ToString() << ", ";
    ss << "\"context\": \"" << context << "\"";
    ss << "}";
    return ss.str();
}

} /* namespace raft */
} /* namespace sharkstore */
