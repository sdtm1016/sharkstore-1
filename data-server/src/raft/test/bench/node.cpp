#include "node.h"

#include <unistd.h>
#include <iostream>

#include "address.h"
#include "config.h"

namespace sharkstore {
namespace raft {
namespace bench {

Node::Node(uint64_t node_id, std::shared_ptr<NodeAddress> addrs)
    : node_id_(node_id), addr_mgr_(addrs) {
    RaftServerOptions ops;
    ops.node_id = node_id_;
    ops.apply_threads_num = bench_config.apply_thread_num;
    ops.consensus_threads_num = bench_config.raft_thread_num;
    ops.election_tick = 2;
    ops.transport_options.listen_port = addr_mgr_->GetListenPort(node_id_);
    ops.transport_options.use_inprocess_transport = false;
    ops.transport_options.resolver =
        std::static_pointer_cast<NodeResolver>(addr_mgr_);
    raft_server_ = CreateRaftServer(ops);
    auto s = raft_server_->Start();
    if (!s.ok()) {
        throw std::runtime_error(std::string("create raft server failed: ") +
                                 s.ToString());
    }
}

Node::~Node() {}

void Node::Start() {
    for (uint64_t i = 1; i <= bench_config.range_num; ++i) {
        auto rng =
            std::make_shared<Range>(i, node_id_, raft_server_.get(), addr_mgr_);
        rng->Start();
        ranges_.emplace(i, rng);
    }
}

std::shared_ptr<Range> Node::GetRange(uint64_t i) {
    auto it = ranges_.find(i);
    if (it != ranges_.end()) {
        return it->second;
    } else {
        throw std::runtime_error(std::string("could not find range ") +
                                 std::to_string(i));
    }
}

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */
