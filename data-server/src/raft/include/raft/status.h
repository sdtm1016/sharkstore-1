_Pragma("once");

#include <stdint.h>
#include <string>
#include <map>

#include "raft/types.h"

namespace sharkstore {
namespace raft {

struct ServerStatus {
    uint64_t total_snap_applying = 0;
    uint64_t total_snap_sending = 0;
};

struct ReplicaStatus {
    Peer peer;
    uint64_t match = 0;
    uint64_t commit = 0;
    uint64_t next = 0;
    int inactive_seconds = 0;
    bool snapshotting = false;
    std::string state;

    std::string ToString() const;
};

struct RaftStatus {
    uint64_t node_id = 0;
    uint64_t leader = 0;
    uint64_t term = 0;
    uint64_t index = 0;  // log index
    uint64_t commit = 0;
    uint64_t applied = 0;
    std::string state;
    // key: node_id
    std::map<uint64_t, ReplicaStatus> replicas;

    RaftStatus() = default;
    RaftStatus& operator=(const RaftStatus& s) = default;

    RaftStatus(RaftStatus&& s);
    RaftStatus& operator=(RaftStatus&& s);

    std::string ToString() const;
};

} /* namespace raft */
} /* namespace sharkstore */
