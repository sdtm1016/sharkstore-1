#include "bulletin_board.h"

namespace sharkstore {
namespace raft {
namespace impl {

using sharkstore::shared_lock;
using sharkstore::shared_mutex;
using std::unique_lock;

void BulletinBoard::PublishLeaderTerm(uint64_t leader, uint64_t term) {
    unique_lock<shared_mutex> lock(mu_);

    leader_ = leader;
    term_ = term;
}

void BulletinBoard::PublishPeers(std::vector<Peer>&& peers) {
    unique_lock<shared_mutex> lock(mu_);

    peers_ = std::move(peers);
}

void BulletinBoard::PublishStatus(RaftStatus&& status) {
    unique_lock<shared_mutex> lock(mu_);

    status_ = std::move(status);
}

void BulletinBoard::LeaderTerm(uint64_t* leader, uint64_t* term) const {
    shared_lock<shared_mutex> lock(mu_);

    *leader = leader_;
    *term = term_;
}

void BulletinBoard::Peers(std::vector<Peer>* peers) const {
    shared_lock<shared_mutex> lock(mu_);

    *peers = peers_;
}

void BulletinBoard::Status(RaftStatus* status) const {
    shared_lock<shared_mutex> lock(mu_);

    *status = status_;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
