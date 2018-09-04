_Pragma("once");

#include <stdint.h>
#include <map>
#include "base/status.h"

#include "../raft.pb.h"
#include "log_format.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

class LogIndex {
public:
    LogIndex();
    ~LogIndex();

    LogIndex(const LogIndex&) = delete;
    LogIndex& operator=(const LogIndex&) = delete;

    // 从Record中还原
    Status ParseFrom(const Record& rec, const std::vector<char>& payload);
    void Serialize(pb::LogIndex* pb_msg);

    size_t Size() const { return items_.size(); }
    bool Empty() const { return items_.empty(); }
    uint64_t First() const;
    uint64_t Last() const;

    uint64_t Term(uint64_t index) const;
    uint32_t Offset(uint64_t index) const;

    void Append(uint64_t index, uint64_t term, uint32_t offset);
    void Truncate(uint64_t index);
    void Clear();

private:
    std::map<uint64_t, pb::IndexItem> items_;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
