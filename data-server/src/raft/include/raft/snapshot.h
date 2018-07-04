_Pragma("once");

#include "base/status.h"

namespace sharkstore {
namespace raft {

class Snapshot {
public:
    Snapshot() = default;
    virtual ~Snapshot() = default;

    Snapshot(const Snapshot&) = delete;
    Snapshot& operator=(const Snapshot&) = delete;

    // 返回kv数据
    virtual Status Next(std::string* data, bool* over) = 0;

    // 返回透传的快照相关的额外数据
    virtual Status Context(std::string* context) = 0;

    // 返回应用位置
    virtual uint64_t ApplyIndex() = 0;

    virtual void Close() = 0;
};

} /* namespace raft */
} /* namespace sharkstore */
