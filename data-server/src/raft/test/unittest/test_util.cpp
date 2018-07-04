#include "test_util.h"

#include "base/util.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace testutil {

using sharkstore::randomInt;
using sharkstore::randomString;

Peer RandomPeer() {
    Peer p;
    p.type = (randomInt() % 2 == 0 ? sharkstore::raft::PeerType::kLearner
                                   : sharkstore::raft::PeerType::kNormal);
    p.node_id = randomInt();
    p.peer_id = randomInt();
    return p;
}

bool EqualPeer(const Peer& p1, const Peer& p2) {
    return p1.type == p2.type && p1.node_id == p2.node_id && p1.peer_id == p2.peer_id;
}

Status Equal(const pb::HardState& lh, const pb::HardState& rh) {
    if (lh.term() != rh.term()) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh.term()) + " != " + std::to_string(rh.term()));
    }
    if (lh.vote() != rh.vote()) {
        return Status(Status::kCorruption, "vote",
                      std::to_string(lh.vote()) + " != " + std::to_string(rh.vote()));
    }
    if (lh.commit() != rh.commit()) {
        return Status(Status::kCorruption, "commit",
                      std::to_string(lh.commit()) + " != " + std::to_string(rh.commit()));
    }
    return Status::OK();
}

Status Equal(const pb::TruncateMeta& lh, const pb::TruncateMeta& rh) {
    if (lh.term() != rh.term()) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh.term()) + " != " + std::to_string(rh.term()));
    }
    if (lh.index() != rh.index()) {
        return Status(Status::kCorruption, "index",
                      std::to_string(lh.index()) + " != " + std::to_string(rh.index()));
    }
    return Status::OK();
}

EntryPtr RandomEntry(uint64_t index, int data_size) {
    EntryPtr e(new sharkstore::raft::impl::pb::Entry);
    e->set_index(index);
    e->set_term(randomInt());
    e->set_type((randomInt() % 2 == 0) ? sharkstore::raft::impl::pb::ENTRY_NORMAL
                                       : sharkstore::raft::impl::pb::ENTRY_CONF_CHANGE);
    e->set_data(randomString(data_size));
    return e;
}

void RandomEntries(uint64_t lo, uint64_t hi, int data_size,
                   std::vector<EntryPtr>* entries) {
    for (uint64_t i = lo; i < hi; ++i) {
        entries->push_back(RandomEntry(i, data_size));
    }
}

Status Equal(const EntryPtr& lh, const EntryPtr& rh) {
    if (lh->index() != rh->index()) {
        return Status(Status::kCorruption, "index",
                      std::to_string(lh->index()) + " != " + std::to_string(rh->index()));
    }
    if (lh->term() != rh->term()) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh->term()) + " != " + std::to_string(rh->term()));
    }
    if (lh->type() != rh->type()) {
        return Status(Status::kCorruption, "type",
                      std::to_string(lh->type()) + " != " + std::to_string(rh->type()));
    }
    if (lh->data() != rh->data()) {
        return Status(Status::kCorruption, "data", lh->data() + " != " + rh->data());
    }

    return Status::OK();
}

Status Equal(const std::vector<EntryPtr>& lh, const std::vector<EntryPtr>& rh) {
    if (lh.size() != rh.size()) {
        return Status(Status::kCorruption, "entries size",
                      std::to_string(lh.size()) + " != " + std::to_string(rh.size()));
    }
    for (size_t i = 0; i < lh.size(); ++i) {
        const auto& le = lh[i];
        const auto& re = rh[i];
        auto s = Equal(le, re);
        if (!s.ok()) {
            return Status(Status::kCorruption,
                          std::string("at index ") + std::to_string(i), s.ToString());
        }
    }
    return Status::OK();
}

} /* namespace testutil */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
