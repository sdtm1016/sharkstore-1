_Pragma("once");

#include <stdint.h>
#include <string>

namespace sharkstore {
namespace raft {

// NodeResolver 用于raft模块获取某个dataserver的ip地址
class NodeResolver {
public:
    NodeResolver() = default;
    virtual ~NodeResolver() = default;

    virtual std::string GetNodeAddress(uint64_t node_id) = 0;
};

} /* namespace raft */
} /* namespace sharkstore */
