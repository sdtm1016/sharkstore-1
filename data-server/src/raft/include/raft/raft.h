_Pragma("once");

#include "options.h"
#include "status.h"

namespace sharkstore {
namespace raft {

class Raft {
public:
    Raft() = default;
    virtual ~Raft() = default;

    Raft(const Raft&) = delete;
    Raft& operator=(const Raft&) = delete;

    virtual bool IsStopped() const = 0;

    virtual void GetLeaderTerm(uint64_t* leader, uint64_t* term) const = 0;
    virtual bool IsLeader() const = 0;

    virtual Status TryToLeader() = 0;

    virtual Status Submit(std::string& cmd) = 0;
    virtual Status ChangeMemeber(const ConfChange& conf) = 0;

    virtual void GetStatus(RaftStatus* status) const = 0;

    virtual void Truncate(uint64_t index) = 0;
};

} /* namespace raft */
} /* namespace sharkstore */
