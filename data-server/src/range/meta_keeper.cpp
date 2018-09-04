#include "meta_keeper.h"

#include <mutex>
#include <sstream>

#include "base/util.h"

namespace sharkstore {
namespace dataserver {
namespace range {

MetaKeeper::MetaKeeper(const metapb::Range& m) : meta_(m) {}

MetaKeeper::MetaKeeper(metapb::Range&& m) : meta_(std::move(m)) {}

metapb::Range MetaKeeper::Get() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    auto result = meta_;
    return result;
}

void MetaKeeper::Get(metapb::Range *out) const {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
    out->CopyFrom(meta_);
}

void MetaKeeper::Set(const metapb::Range& to) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
    meta_ = to;
}

void MetaKeeper::Set(metapb::Range&& to) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
    meta_ = std::move(to);
}

void MetaKeeper::GetEpoch(metapb::RangeEpoch* epoch) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    epoch->CopyFrom(meta_.range_epoch());
}

uint64_t MetaKeeper::GetConfVer() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.range_epoch().conf_ver();
}

uint64_t MetaKeeper::GetVersion() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.range_epoch().version();
}

uint64_t MetaKeeper::GetTableID() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.table_id();
}

std::string MetaKeeper::GetStartKey() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.start_key();
}

std::string MetaKeeper::GetEndKey() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.end_key();
}

Status MetaKeeper::verifyConfVer(uint64_t conf_ver) const {
    uint64_t current = meta_.range_epoch().conf_ver();
    if (conf_ver == current) {
        return Status::OK();
    }

    std::ostringstream ss;
    ss << "current is: " << current;
    ss << ", to verify is: " << conf_ver;

    if (conf_ver < current) {
        return Status(Status::kStaleEpoch, "conf ver", ss.str());
    } else {
        assert(conf_ver != current);
        return Status(Status::kInvalidArgument, "conf ver", ss.str());
    }
}

Status MetaKeeper::AddPeer(const metapb::Peer& peer, uint64_t verify_conf_ver) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    auto s = verifyConfVer(verify_conf_ver);
    if (!s.ok()) return s;

    // check peer already exist
    for (const auto& old : meta_.peers()) {
        if (old.node_id() == peer.node_id() || old.id() == peer.id()) {
            return Status(Status::kExisted,
                    std::string("old is ") + old.ShortDebugString(),
                    std::string("to add is ") + peer.ShortDebugString());
        }
    }

    // add
    meta_.add_peers()->CopyFrom(peer);
    meta_.mutable_range_epoch()->set_conf_ver(verify_conf_ver + 1);

    return Status::OK();
}

Status MetaKeeper::DelPeer(const metapb::Peer& peer, uint64_t verify_conf_ver) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    auto s = verifyConfVer(verify_conf_ver);
    if (!s.ok()) return s;

    // find and delete
    for (auto it = meta_.peers().cbegin(); it != meta_.peers().cend(); ++it) {
        auto same_node = it->node_id() == peer.node_id();
        auto same_id = it->id() == peer.id();
        if (same_node && same_id) {
            meta_.mutable_peers()->erase(it);
            meta_.mutable_range_epoch()->set_conf_ver(verify_conf_ver + 1);
            return Status::OK();
        }
    }
    return Status(Status::kNotFound);
}

Status MetaKeeper::PromotePeer(uint64_t node_id, uint64_t peer_id) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    for (int i = 0; i < meta_.peers_size(); ++i) {
        auto mp = meta_.mutable_peers(i);
        if (mp->id() == peer_id && mp->node_id() == node_id) {
            mp->set_type(metapb::PeerType_Normal);
            return Status::OK();
        }
    }

    return Status(Status::kNotFound);
}

std::vector<metapb::Peer> MetaKeeper::GetAllPeers() const {
    std::vector<metapb::Peer> peers;
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

        for (const auto& p : meta_.peers()) {
            peers.emplace_back(p);
        }
    }
    return peers;
}

bool MetaKeeper::FindPeer(uint64_t peer_id, metapb::Peer* peer) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    for (const auto& p: meta_.peers()) {
        if (p.id() == peer_id) {
            if (peer != nullptr) peer->CopyFrom(p);
            return true;
        }
    }
    return false;
}

bool MetaKeeper::FindPeerByNodeID(uint64_t node_id, metapb::Peer* peer) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    for (const auto& p: meta_.peers()) {
        if (p.node_id() == node_id) {
            if (peer != nullptr) peer->CopyFrom(p);
            return true;
        }
    }
    return false;
}

Status MetaKeeper::verifyVersion(uint64_t version) const {
    uint64_t current = meta_.range_epoch().version();
    if (version == current) {
        return Status::OK();
    }

    std::ostringstream ss;
    ss << "current is: " << current;
    ss << ", to verify is: " << version;

    if (version < current) {
        return Status(Status::kStaleEpoch, "conf ver", ss.str());
    } else {
        assert(version != current);
        return Status(Status::kInvalidArgument, "conf ver", ss.str());
    }
}

Status MetaKeeper::CheckSplit(const std::string& end_key, uint64_t version) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    // check end key valid
    if (end_key >= meta_.end_key() || end_key <= meta_.start_key()) {
        std::ostringstream ss;
        ss << EncodeToHex(end_key) << " out of bound [ ";
        ss << EncodeToHex(meta_.start_key()) << " - ";
        ss << EncodeToHex(meta_.end_key()) << "]";
        return Status(Status::kOutOfBound, "split end key", ss.str());
    }
    // check version
    return verifyVersion(version);
}

void MetaKeeper::Split(const std::string& end_key, uint64_t new_version) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    meta_.set_end_key(end_key);
    meta_.mutable_range_epoch()->set_version(new_version);
}

std::string MetaKeeper::ToString() const {
    std::string s;
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
        s = meta_.ShortDebugString();
    }
    return s;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
