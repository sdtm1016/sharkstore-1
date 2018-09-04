_Pragma("once");

#include "base/shared_mutex.h"
#include "base/status.h"
#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace range {

// MetaKeeper:
// manager query and update operation to metapb::Range
class MetaKeeper {
public:
    explicit MetaKeeper(const metapb::Range& m);
    explicit MetaKeeper(metapb::Range&& m);
    ~MetaKeeper() = default;

    MetaKeeper(const MetaKeeper&) = delete;
    MetaKeeper& operator=(const MetaKeeper&) = delete;

    metapb::Range Get() const;
    void Get(metapb::Range *out) const;
    void Set(const metapb::Range& to);
    void Set(metapb::Range&& to);

    uint64_t GetTableID() const;
    std::string GetStartKey() const;
    std::string GetEndKey() const;

    void GetEpoch(metapb::RangeEpoch* epoch) const;
    uint64_t GetConfVer() const;
    uint64_t GetVersion() const;

    Status AddPeer(const metapb::Peer& peer, uint64_t verify_conf_ver);
    Status DelPeer(const metapb::Peer& peer, uint64_t verify_conf_ver);
    Status PromotePeer(uint64_t node_id, uint64_t peer_id);

    std::vector<metapb::Peer> GetAllPeers() const;
    bool FindPeer(uint64_t peer_id, metapb::Peer* peer = nullptr) const;
    bool FindPeerByNodeID(uint64_t node_id, metapb::Peer* peer = nullptr) const;

    Status CheckSplit(const std::string& end_key, uint64_t version) const;
    void Split(const std::string& end_key, uint64_t new_version);

    std::string ToString() const;

private:
    // unlocked
    Status verifyConfVer(uint64_t conf_ver) const;
    // unlocked
    Status verifyVersion(uint64_t version) const;

private:
    mutable shared_mutex rw_lock_;
    metapb::Range meta_;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
