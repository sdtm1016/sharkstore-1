_Pragma("once");

#include <memory>

#include "base/status.h"
#include "raft_log_unstable.h"
#include "raft_types.h"

namespace sharkstore {
namespace raft {
namespace impl {

namespace storage {
class Storage;
};

class RaftLog {
public:
    explicit RaftLog(uint64_t id, const std::shared_ptr<storage::Storage>& s);
    ~RaftLog() = default;

    RaftLog(const RaftLog&) = delete;
    RaftLog& operator=(const RaftLog&) = delete;

    uint64_t committed() const { return committed_; }
    void set_committed(uint64_t commit) { committed_ = commit; }
    uint64_t applied() const { return applied_; }

    uint64_t firstIndex() const;
    uint64_t lastIndex() const;

    Status term(uint64_t index, uint64_t* term) const;
    uint64_t lastTerm() const;
    void lastIndexAndTerm(uint64_t* index, uint64_t* term) const;
    bool matchTerm(uint64_t index, uint64_t term) const;

    uint64_t findConfilct(const std::vector<EntryPtr>& ents) const;

    bool maybeAppend(uint64_t index, uint64_t log_term, uint64_t commit,
                     const std::vector<EntryPtr>& ents, uint64_t* lastnewi);
    uint64_t append(const std::vector<EntryPtr>& ents);

    // 返回需要持久化的日志
    // TODO: return const references
    void unstableEntries(std::vector<EntryPtr>* ents);

    // 返回已提交需要被应用的日志
    void nextEntries(uint64_t max_size, std::vector<EntryPtr>* ents);

    // 返回从index开始的日志
    Status entries(uint64_t index, uint64_t max_size, std::vector<EntryPtr>* ents) const;

    // 检查 index 和 term是否匹配，若匹配则尝试更新commit
    bool maybeCommit(uint64_t max_index, uint64_t term);

    // 更新commit
    void commitTo(uint64_t commit);

    // 更新applied
    void appliedTo(uint64_t index);

    // 持久化（删除unstable里的)
    void stableTo(uint64_t index, uint64_t term);

    // 处理投票请求时，检查请求者的日志是否足够新
    bool isUpdateToDate(uint64_t lasti, uint64_t term);

    // 应用快照
    void restore(uint64_t index);

    Status slice(uint64_t lo, uint64_t hi, uint64_t max_size,
                 std::vector<EntryPtr>* ents) const;

    void allEntries(std::vector<EntryPtr>* ents) const;

private:
    static uint64_t zeroTermOnErrCompacted(uint64_t term, const Status& s);

    Status open();

    Status mustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

private:
    const uint64_t id_;
    std::shared_ptr<storage::Storage> storage_;
    std::unique_ptr<UnstableLog> unstable_;
    uint64_t committed_{0};
    uint64_t applied_{0};
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
