#ifndef __RAFT_SERVER_MOCK_H__
#define __RAFT_SERVER_MOCK_H__

#include "base/status.h"
#include "raft/server.h"
#include "raft_mock.h"

class RaftServerMock : public raft::RaftServer {
public:
    Status Start() override { return Status::OK(); }
    Status Stop() override { return Status::OK(); }
    const RaftServerOptions& Options() const { return rop_; }

    Status CreateRaft(const RaftOptions&, std::shared_ptr<Raft>* raft) override ;

    Status RemoveRaft(uint64_t id) override { return Status::OK(); }

    Status DestroyRaft(uint64_t id, bool backup) override { return Status::OK(); }

    std::shared_ptr<Raft> FindRaft(uint64_t id) const override {
        return std::shared_ptr<Raft>(nullptr);
    }

    void GetStatus(ServerStatus* status) const override {}

private:
    RaftServerOptions rop_;
};
#endif  //__RAFT_SERVER_MOCK_H__
