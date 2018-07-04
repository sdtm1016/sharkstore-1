#ifndef __RAFT_SERVER_MOCK_H__
#define __RAFT_SERVER_MOCK_H__

#include "base/status.h"
#include "raft/server.h"
#include "raft_mock.h"

class RaftServerMock : public raft::RaftServer {
public:
    virtual Status Start() { return Status::OK(); }
    virtual Status Stop() { return Status::OK(); }
    virtual const RaftServerOptions& Options() const { return rop_; }

    virtual Status CreateRaft(const RaftOptions&, std::shared_ptr<Raft>* raft);

    virtual Status RemoveRaft(uint64_t id, bool bakup) { return Status::OK(); }

    virtual std::shared_ptr<Raft> FindRaft(uint64_t id) const {
        return std::shared_ptr<Raft>(nullptr);
    }

    void GetStatus(ServerStatus* status) const override {}

private:
    RaftServerOptions rop_;
};
#endif  //__RAFT_SERVER_MOCK_H__
