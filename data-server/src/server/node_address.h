#ifndef __NODE_ADDRESS_H__
#define __NODE_ADDRESS_H__

#include <mutex>
#include <string>
#include <unordered_map>

#include "raft/options.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class NodeAddress : public raft::NodeResolver {
public:
    NodeAddress() = default;
    virtual ~NodeAddress() = default;

    NodeAddress(const NodeAddress&) = delete;
    NodeAddress& operator=(const NodeAddress&) = delete;
    NodeAddress& operator=(const NodeAddress&) volatile = delete;

    virtual std::string GetNodeAddress(uint64_t node_id);

private:
    std::mutex mutex_;
    std::unordered_map<uint64_t, std::string> address_map_;
};

}//namespace server
}//namespace dataserver
}//namespace sharkstore

#endif//__NODE_ADDRESS_H__
