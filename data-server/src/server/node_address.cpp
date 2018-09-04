#include "node_address.h"

#include "context_server.h"
#include "frame/sf_logger.h"
#include "master/worker.h"
#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

NodeAddress::NodeAddress(master::Worker *master_worker)
    : master_server_(master_worker) {
}

std::string NodeAddress::GetNodeAddress(uint64_t node_id) {
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(mutex_);
        auto it = address_map_.find(node_id);
        if (it != address_map_.end()) {
            return it->second;
        }
    }

    std::string addr;
    auto s = master_server_->GetRaftAddress(node_id, &addr);
    if (!s.ok()) {
        FLOG_ERROR("get raft address to %" PRIu64 " failed: %s",  node_id, s.ToString().c_str());
    } else if (!addr.empty()) {
        std::unique_lock<sharkstore::shared_mutex> lock(mutex_);
        address_map_[node_id] = addr;
    }

    return addr;
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
