_Pragma("once");

#include <memory>
#include <vector>
#include "base/status.h"
#include "raft/snapshot.h"
#include "raft/types.h"

namespace sharkstore {
namespace raft {

class StateMachine {
public:
    StateMachine() = default;
    virtual ~StateMachine() = default;

    StateMachine(const StateMachine&) = delete;
    StateMachine& operator=(const StateMachine&) = delete;

    virtual Status Apply(const std::string& cmd, uint64_t index) = 0;
    virtual Status ApplyMemberChange(const ConfChange& cc, uint64_t index) = 0;

    // raft复制命令时发生错误，如当前节点不是leader等
    virtual void OnReplicateError(const std::string& cmd, const Status& status) = 0;

    virtual void OnLeaderChange(uint64_t leader, uint64_t term) = 0;

    // TODO: use unique_ptr?
    virtual std::shared_ptr<Snapshot> GetSnapshot() = 0;

    virtual Status ApplySnapshotStart(const std::string& context) = 0;
    virtual Status ApplySnapshotData(const std::vector<std::string>& datas) = 0;
    virtual Status ApplySnapshotFinish(uint64_t index) = 0;
};

} /* namespace raft */
} /* namespace sharkstore */
