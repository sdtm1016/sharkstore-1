#include "raft_server_mock.h"

Status RaftServerMock::CreateRaft(const RaftOptions& ops, std::shared_ptr<Raft>* raft) {
    auto r = std::make_shared<RaftMock>(ops);
    *raft = std::static_pointer_cast<Raft>(r);

    return Status::OK();
}
