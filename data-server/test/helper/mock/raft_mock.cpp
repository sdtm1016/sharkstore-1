#include "raft_mock.h"

RaftMock::RaftMock(const RaftOptions& ops) : ops_(ops) {}

Status RaftMock::Submit(std::string& cmd) {
    ops_.statemachine->Apply(cmd, 1);
    return Status::OK();
}

Status RaftMock::ChangeMemeber(const ConfChange& conf) { return Status::OK(); }

void RaftMock::SetLeaderTerm(uint64_t leader, uint64_t term) {
    leader_ = leader;
    term_ = term;
}

void RaftMock::GetLeaderTerm(uint64_t* leader, uint64_t* term) const {
    *leader = leader_;
    *term = term_;
}

bool RaftMock::IsLeader() const {
    // 默认1就是本节点
    // TODO: 使用构造函数传递本节点NodeId
    return leader_ == 1;
}
