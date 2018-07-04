_Pragma("once");

#include <chrono>
#include <memory>
#include "base/status.h"
#include "raft/types.h"

#include "raft.pb.h"

namespace sharkstore {
namespace raft {
namespace impl {

static const uint64_t kNoLimit = std::numeric_limits<uint64_t>::max();

using EntryPtr = std::shared_ptr<pb::Entry>;
using MessagePtr = std::shared_ptr<pb::Message>;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

enum class FsmState { kFollower = 0, kCandidate, kLeader, kPreCandidate };

std::string FsmStateName(FsmState state);

enum class ReplicaState { kProbe = 0, kReplicate, kSnapshot };

std::string ReplicateStateName(ReplicaState state);

// encode to pb  or  decode from pb
Status EncodePeer(const Peer& peer, pb::Peer* pb_peer);
Status DecodePeer(const pb::Peer& pb_peer, Peer* peer);

// encode to pb string or  decode from pb string
Status EncodeConfChange(const ConfChange& cc, std::string* pb_str);
Status DecodeConfChange(const std::string& pb_str, ConfChange* cc);

bool IsLocalMsg(MessagePtr& msg);
bool IsResponseMsg(MessagePtr& msg);

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
