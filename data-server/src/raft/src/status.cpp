#include "raft/status.h"

#include <sstream>

namespace sharkstore {
namespace raft {

std::string ReplicaStatus::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"peer\": " << peer.ToString() << ", ";
    ss << "\"match\": " << match << ", ";
    ss << "\"commit\": " << commit << ", ";
    ss << "\"next\": " << next << ", ";
    ss << "\"inactive\": " << inactive_seconds << ", ";
    ss << "\"state\": \"" << state << "\"";
    ss << "}";
    return ss.str();
}

RaftStatus::RaftStatus(RaftStatus&& s) { *this = std::move(s); }

RaftStatus& RaftStatus::operator=(RaftStatus&& s) {
    if (this != &s) {
        node_id = std::move(s.node_id);
        leader = std::move(s.leader);
        term = std::move(s.term);
        index = std::move(s.index);
        commit = std::move(s.commit);
        applied = std::move(s.applied);
        state = std::move(s.state);
        replicas = std::move(s.replicas);
    }
    return *this;
}

std::string RaftStatus::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"node_id\": " << node_id << ", "
       << "\"leader\": " << leader << ", "
       << "\"term\": " << term << ", "
       << "\"index\": " << index << ", "
       << "\"commit\": " << commit << ", "
       << "\"applied\": " << applied << ", "
       << "\"state\": "
       << "\"" << state << "\", "
       << "\"replicas\":";
    ss << "[";
    for (auto it = replicas.cbegin(); it != replicas.cend(); ++it) {
        ss << "{\"" << it->first << "\": " << it->second.ToString() << "}";
        if (std::next(it) != replicas.cend()) {
            ss << ", ";
        }
    }
    ss << "]}";
    return ss.str();
}

} /* namespace raft */
} /* namespace sharkstore */
