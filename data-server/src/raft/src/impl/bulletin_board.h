_Pragma("once");

#include <atomic>
#include <mutex>
#include <vector>
#include "base/shared_mutex.h"
#include "raft.pb.h"
#include "raft/status.h"
#include "raft/types.h"

namespace sharkstore {
namespace raft {
namespace impl {

// BulletinBoard: exhibit fsm's newest status
class BulletinBoard {
public:
    BulletinBoard() = default;
    ~BulletinBoard() = default;

    BulletinBoard(const BulletinBoard&) = delete;
    BulletinBoard& operator=(const BulletinBoard&) = delete;

    void PublishLeaderTerm(uint64_t leader, uint64_t term);
    void PublishPeers(std::vector<Peer>&& peers);
    void PublishStatus(RaftStatus&& status);

    uint64_t Leader() const { return leader_; }
    uint64_t Term() const { return term_; }
    void LeaderTerm(uint64_t* leader, uint64_t* term) const;

    void Peers(std::vector<Peer>* peers) const;

    void Status(RaftStatus* status) const;

private:
    std::atomic<uint64_t> leader_ = {0};
    std::atomic<uint64_t> term_ = {0};
    std::vector<Peer> peers_;
    RaftStatus status_;
    mutable sharkstore::shared_mutex mu_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
