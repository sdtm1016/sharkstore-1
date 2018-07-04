_Pragma("once");

#include "raft_types.h"
#include "snapshot/apply_task.h"
#include "snapshot/send_task.h"

namespace sharkstore {
namespace raft {
namespace impl {

struct Ready {
    // committed entries, can apply to statemachine
    std::vector<EntryPtr> committed_entries;

    // msgs to send
    std::vector<MessagePtr> msgs;

    // snapshot to send
    std::shared_ptr<SendSnapTask> send_snap;

    // snapshot to apply
    std::shared_ptr<ApplySnapTask> apply_snap;

    /* // change list about peers */
    /* std::vector<Peer> pendings_peers; */
    /* std::vector<DownPeers> down_peers; */
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
