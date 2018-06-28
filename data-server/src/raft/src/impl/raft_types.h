_Pragma("once");

#include <memory>
#include <functional>
#include "raft/types.h"

#include "raft.pb.h"

namespace fbase {
namespace raft {
namespace impl {

static const uint64_t kNoLimit = std::numeric_limits<uint64_t>::max();

typedef std::shared_ptr<pb::Entry> EntryPtr;
typedef std::shared_ptr<pb::Message> MessagePtr;

enum class FsmState : char {
    kFollower = 0,
    kCandidate,
    kLeader,
    kPreCandidate
};
std::string FsmStateName(FsmState state);

enum class ReplicaState : char { kProbe = 0, kReplicate, kSnapshot };
std::string ReplicateStateName(ReplicaState state);

pb::Peer toPBPeer(Peer p);
Peer fromPBPeer(pb::Peer p);

void toPBCC(const ConfChange& cc, pb::ConfChange* pb_cc);
void fromPBCC(const pb::ConfChange& pb_cc, ConfChange* cc);

std::string MessageToString(const pb::Message& msg);

bool IsLocalMsg(MessagePtr& msg);
bool IsResponseMsg(MessagePtr& msg);

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
