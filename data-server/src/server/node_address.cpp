#include "node_address.h"

#include "context_server.h"
#include "frame/sf_logger.h"
#include "master/worker.h"
#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

std::string NodeAddress::GetNodeAddress(uint64_t node_id) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = address_map_.find(node_id);
        if (it != address_map_.end()) {
            return it->second;
        }
    }

    auto gw = DataServer::Instance().context_server();
    std::string addr;
    auto s = gw->master_worker->GetRaftAddress(node_id, &addr);
    if (!s.ok()) {
        FLOG_ERROR("get raft address to %lu failed(%s).", node_id,
                   s.ToString().c_str());
    } else if (!addr.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        address_map_[node_id] = addr;
    }

    return addr;
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
