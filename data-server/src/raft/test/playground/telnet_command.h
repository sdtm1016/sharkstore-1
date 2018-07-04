_Pragma("once");

#include <functional>
#include <string>
#include <vector>

namespace sharkstore {
namespace raft {
namespace playground {

typedef std::function<std::string(const std::vector<std::string>&)>
    CommandHandler;

struct TelnetCommand {
    size_t argc;  // 命令至少需要几个参数
    CommandHandler handler;
    std::string usage;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */
