_Pragma("once");

#include "base/status.h"
#include "raft/src/impl/raft_types.h"
#include "raft/src/impl/snapshot/types.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace testutil {

Peer RandomPeer();
bool EqualPeer(const Peer& p1, const Peer& p2);

Status Equal(const pb::HardState& lh, const pb::HardState& rh);
Status Equal(const pb::TruncateMeta& lh, const pb::TruncateMeta& rh);

EntryPtr RandomEntry(uint64_t index, int data_size = 64);
Status Equal(const EntryPtr& lh, const EntryPtr& rh);

void RandomEntries(uint64_t lo, uint64_t hi, int data_size,
                   std::vector<EntryPtr>* entries);
Status Equal(const std::vector<EntryPtr>& lh, const std::vector<EntryPtr>& rh);


SnapContext randSnapContext();
Status Equal(const SnapContext& lh, const SnapContext& rh);

} /* namespace testutil */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
