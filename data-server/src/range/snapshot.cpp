#include "snapshot.h"

namespace sharkstore {
namespace dataserver {
namespace range {

Snapshot::Snapshot(uint64_t applied, raft_cmdpb::SnapshotContext&& ctx,
                   storage::Iterator* iter)
    : applied_(applied), context_(ctx), iter_(iter) {}

Snapshot::~Snapshot() { Close(); }

Status Snapshot::Next(std::string* data, bool* over) {
    if (iter_->Valid()) {
        raft_cmdpb::SnapshotKVPair p;
        p.set_key(iter_->key());
        p.set_value(iter_->value());
        if (!p.SerializeToString(data)) {
            return Status(Status::kCorruption, "serialize snapshot data",
                          "pb return false");
        }
        *over = false;
        iter_->Next();
    } else {
        *over = true;
    }
    return iter_->status();
}

Status Snapshot::Context(std::string* context) {
    if (!context_.SerializeToString(context)) {
        return Status(Status::kCorruption, "serialize snapshot meta", "pb return false");
    }
    return Status::OK();
}

uint64_t Snapshot::ApplyIndex() { return applied_; }

void Snapshot::Close() {
    delete iter_;
    iter_ = nullptr;
}

} /* namespace range */
} /* namespace dataserver */
} /* namespace sharkstore */