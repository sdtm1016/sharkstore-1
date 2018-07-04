_Pragma("once");

#include <functional>
#include "base/status.h"

namespace sharkstore {
namespace raft {
namespace impl {

struct SnapContext {
    uint64_t id = 0;
    uint64_t from = 0;
    uint64_t to = 0;
    uint64_t term = 0;
    uint64_t uuid = 0;
};

struct SnapResult {
    Status status;            // 执行结果, 出错情况
    size_t blocks_count = 0;  // 发送多少数据块
    size_t bytes_count = 0;   // 发送多少字节数
};

using SnapReporter = std::function<void(const SnapContext&, const SnapResult&)>;


} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
