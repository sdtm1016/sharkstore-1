_Pragma("once");

#include <mutex>
#include <string>
#include <unordered_map>
#include <master/worker.h>

#include "base/shared_mutex.h"
#include "raft/options.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class NodeAddress : public raft::NodeResolver {
public:
    explicit NodeAddress(master::Worker *master_worker);
    virtual ~NodeAddress() = default;

    NodeAddress(const NodeAddress&) = delete;
    NodeAddress& operator=(const NodeAddress&) = delete;
    NodeAddress& operator=(const NodeAddress&) volatile = delete;

    virtual std::string GetNodeAddress(uint64_t node_id);

private:
    shared_mutex mutex_;
    std::unordered_map<uint64_t, std::string> address_map_;
    master::Worker* master_server_;
};

}//namespace server
}//namespace dataserver
}//namespace sharkstore
