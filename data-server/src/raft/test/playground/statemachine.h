_Pragma("once");

#include "raft/statemachine.h"

namespace sharkstore {
namespace raft {
namespace playground {

class PGStateMachine : public StateMachine {
public:
    PGStateMachine();
    ~PGStateMachine();

    uint64_t sum() const { return sum_; }

    Status Apply(const std::string& cmd, uint64_t index) override;
    Status ApplyMemberChange(const ConfChange& cc, uint64_t index) override;

    void OnReplicateError(const std::string& cmd,
                          const Status& status) override;

    void OnLeaderChange(uint64_t leader, uint64_t term) override;

    std::shared_ptr<Snapshot> GetSnapshot() override;

    Status ApplySnapshotStart(const std::string&) override;
    Status ApplySnapshotData(const std::vector<std::string>& data) override;
    virtual Status ApplySnapshotFinish(uint64_t index) override;

private:
    uint64_t sum_{0};
    uint64_t applied_{0};
};

class PGSnapshot : public Snapshot {
public:
    PGSnapshot(uint64_t sum, uint64_t applied) : sum_(sum), applied_(applied) {}

    ~PGSnapshot() {}

    Status Next(std::string* data, bool* over) override  {
        data->assign(std::to_string(sum_));
        *over = count_ >= 10;
        ++count_;
        return Status::OK();
    }

    Status Context(std::string*) override { return Status::OK(); }

    uint64_t ApplyIndex() override { return applied_; }

    void Close() override {}

private:
    uint64_t sum_{0};
    uint64_t applied_{0};
    uint64_t count_{0};
};

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */
