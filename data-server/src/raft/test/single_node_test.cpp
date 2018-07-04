#include <unistd.h>
#include <cassert>
#include <iostream>

#include "number_statemachine.h"
#include "raft/raft.h"
#include "raft/server.h"

using namespace sharkstore;
using namespace sharkstore::raft;

int main(int argc, char* argv[]) {
    // TODO:
    RaftServerOptions ops;
    ops.node_id = 1;
    ops.election_tick = 2;
    ops.tick_interval = std::chrono::milliseconds(100);
    ops.transport_options.use_inprocess_transport = true;

    auto rs = CreateRaftServer(ops);
    assert(rs);
    auto s = rs->Start();
    assert(s.ok());

    auto sm = std::make_shared<raft::test::NumberStateMachine>(1);

    RaftOptions rops;
    rops.id = 9;
    rops.statemachine = sm;
    rops.use_memory_storage = true;
    Peer p;
    p.type = PeerType::kNormal;
    p.node_id = 1;
    rops.peers.push_back(p);

    std::shared_ptr<Raft> r;
    s = rs->CreateRaft(rops, &r);
    assert(s.ok());

    while (!r->IsLeader()) {
        usleep(1000 * 100);
    }

    std::cout << "leader elected." << std::endl;

    for (int i = 0; i <= 100; ++i) {
        std::string cmd = std::to_string(i);
        s = r->Submit(cmd);
        assert(s.ok());
    }

    s = sm->WaitNumber(100);
    assert(s.ok());
    std::cout << "wait successfully." << std::endl;
}
