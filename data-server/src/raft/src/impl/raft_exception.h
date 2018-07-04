_Pragma("once");

#include <exception>

#include "base/status.h"

namespace sharkstore {
namespace raft {
namespace impl {

class RaftException : public std::runtime_error {
public:
    explicit RaftException(const std::string& s) : std::runtime_error(s) {}
    explicit RaftException(const Status& s)
        : std::runtime_error(s.ToString()) {}

    ~RaftException() throw() {}
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
