#ifndef __RAFT_MOCK_H__
#define __RAFT_MOCK_H__

#include "raft/raft.h"

using namespace sharkstore;
using namespace sharkstore::raft;

class RaftMock : public Raft {
public:
    RaftMock(const RaftOptions& ops);

    bool IsStopped() const override { return false; }

    void SetLeaderTerm(uint64_t leader, uint64_t term);
    void GetLeaderTerm(uint64_t* leader, uint64_t* term) const override;
    bool IsLeader() const override;
    Status TryToLeader() override { return Status::OK(); }

    Status Submit(std::string& cmd) override ;
    Status ChangeMemeber(const ConfChange& conf) override ;

    void GetStatus(RaftStatus* status) const override {}

    void Truncate(uint64_t index) override {}

private:
    RaftOptions ops_;
    uint64_t leader_ = 0;
    uint64_t term_ = 0;
};
#endif  //__RAFT_MOCK_H__
