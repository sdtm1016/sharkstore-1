#include "raft_mock.h"

RaftMock::RaftMock(const RaftOptions& ops) : ops_(ops) {}

Status RaftMock::Submit(std::string& cmd) {
    ops_.statemachine->Apply(cmd, 1);
    return Status::OK();
}

Status RaftMock::ChangeMemeber(const ConfChange& conf) { return Status::OK(); }

void RaftMock::GetLeaderTerm(uint64_t* leader, uint64_t* term) const {
    *leader = ops_.leader;
    *term = ops_.term;
}
