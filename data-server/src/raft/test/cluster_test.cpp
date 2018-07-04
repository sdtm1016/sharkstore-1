#include <unistd.h>
#include <cassert>
#include <iostream>
#include <thread>

#include "number_statemachine.h"
#include "raft/raft.h"
#include "raft/server.h"

using namespace sharkstore;
using namespace sharkstore::raft;

static const size_t kNodeNum = 5;

std::condition_variable g_cv;
std::mutex g_mu;
size_t g_finish_count = 0;
std::vector<Peer> g_peers;

void run_node(uint64_t node_id) {
    RaftServerOptions ops;
    ops.node_id = node_id;
    ops.tick_interval = std::chrono::milliseconds(100);
    ops.election_tick = 5;
    ops.transport_options.use_inprocess_transport = true;

    auto rs = CreateRaftServer(ops);
    assert(rs);
    auto s = rs->Start();
    assert(s.ok());

    auto sm = std::make_shared<raft::test::NumberStateMachine>(node_id);

    RaftOptions rops;
    rops.id = 1;
    rops.statemachine = sm;
    rops.use_memory_storage = true;
    rops.peers = g_peers;

    std::shared_ptr<Raft> r;
    s = rs->CreateRaft(rops, &r);
    std::cout << s.ToString() << std::endl;
    assert(s.ok());

    while (true) {
        uint64_t leader;
        uint64_t term;
        r->GetLeaderTerm(&leader, &term);
        if (leader != 0) {
            break;
        } else {
            usleep(1000 * 100);
        }
    }

    int reqs_count = 10000;
    if (r->IsLeader()) {
        for (int i = 1; i <= reqs_count; ++i) {
            std::string cmd = std::to_string(i);
            s = r->Submit(cmd);
            assert(s.ok());
        }
    }

    s = sm->WaitNumber(reqs_count);
    assert(s.ok());

    std::cout << "[NODE" << node_id << "]"
              << " wait number return: " << s.ToString() << std::endl;

    r->Truncate(3);

    // 本节点任务完成
    {
        std::lock_guard<std::mutex> lock(g_mu);
        ++g_finish_count;
    }
    g_cv.notify_all();

    // 等待所有节点完成，退出
    std::unique_lock<std::mutex> lock(g_mu);
    while (g_finish_count < kNodeNum) {
        g_cv.wait(lock);
    }
};

// 测试顺序 5个节点
//
// 1) 启动2个普通节点+1个learner节点
// 2) 等待复制完成以及日志截断
// 3) 启动剩下的1个普通节点 + 1个learner节点，验证快照逻辑

int main(int argc, char* argv[]) {
    if (kNodeNum < 5) {
        throw std::runtime_error("node number should greater than five.");
    }

    // 初始化集群成员
    for (uint64_t i = 1; i <= kNodeNum; ++i) {
        Peer p;
        if (i == 3 || i == kNodeNum - 1) {
            p.type = PeerType::kLearner;
        } else {
            p.type = PeerType::kNormal;
        }
        p.node_id = i;
        p.peer_id = i;
        g_peers.push_back(p);
    }

    std::vector<std::thread> threads;

    // 先启动n-2个节点
    for (uint64_t i = 1; i <= kNodeNum - 2; ++i) {
        threads.push_back(std::thread(std::bind(&run_node, i)));
    }

    // 等待n-2个节点复制完成和日志截断
    {
        std::unique_lock<std::mutex> lock(g_mu);
        while (g_finish_count < kNodeNum - 2) {
            g_cv.wait(lock);
        }
    }

    // 启动最后两个节点，一个普通，一个learner 验证快照逻辑
    threads.push_back(std::thread(std::bind(&run_node, kNodeNum - 1)));
    threads.push_back(std::thread(std::bind(&run_node, kNodeNum)));

    for (auto& t : threads) {
        t.join();
    }
}
