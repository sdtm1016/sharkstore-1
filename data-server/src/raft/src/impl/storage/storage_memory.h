_Pragma("once");

#include <deque>
#include "storage.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

class MemoryStorage : public Storage {
public:
    MemoryStorage(uint64_t id, uint64_t capacity);
    ~MemoryStorage();

    Status Open() override;

    Status InitialState(pb::HardState* hs) const override;

    Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                   std::vector<EntryPtr>* entries, bool* is_compacted) const override;

    Status Term(uint64_t index, uint64_t* term, bool* is_compacted) const override;

    Status FirstIndex(uint64_t* index) const override;
    Status LastIndex(uint64_t* index) const override;

    Status StoreEntries(const std::vector<EntryPtr>& entries) override;

    Status StoreHardState(const pb::HardState& hs) override;

    Status Truncate(uint64_t index) override;

    Status ApplySnapshot(const pb::SnapshotMeta& meta) override;

    void AppliedTo(uint64_t applied) override {}  // TODO:

    Status Close() override;

    Status Destroy() override;

    Status Backup() override;

private:
    uint64_t lastIndex() const;
    void truncateTo(uint64_t index);

private:
    const uint64_t id_ = 0;
    const uint64_t capacity_ = 0;

    uint64_t trunc_index_ = 0;
    uint64_t trunc_term_ = 0;

    std::deque<EntryPtr> entries_;

    pb::HardState hardstate_;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
