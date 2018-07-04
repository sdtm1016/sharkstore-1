#ifndef __RAFT_MOCK_H__
#define __RAFT_MOCK_H__

#include "raft/raft.h"

using namespace sharkstore;
using namespace sharkstore::raft;

class RaftMock : public Raft {
public:
    RaftMock(const RaftOptions& ops);

    virtual bool IsStopped() const { return false; }

    virtual void GetLeaderTerm(uint64_t* leader, uint64_t* term) const;
    virtual bool IsLeader() const { return true; }
    virtual Status TryToLeader() { return Status::OK(); }

    virtual Status Submit(std::string& cmd);
    virtual Status ChangeMemeber(const ConfChange& conf);

    virtual void GetPeers(std::vector<Peer>* peers) const {}
    virtual void GetDownPeers(std::vector<DownPeer>* peers) const {}
    virtual void GetPeedingPeers(std::vector<Peer>* peers) const {}
    virtual void GetStatus(RaftStatus* status) const {}

    virtual void Truncate(uint64_t index) {}

private:
    RaftOptions ops_;
};
#endif  //__RAFT_MOCK_H__
