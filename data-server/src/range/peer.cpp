#include "range.h"

#include "storage/meta_store.h"
#include "proto/gen/raft_cmdpb.pb.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

uint64_t Range::GetPeerID() const {
    metapb::Peer peer;
    if (meta_.FindPeerByNodeID(node_id_)) {
        return peer.id();
    }
    return 0;
}

void Range::AddPeer(const metapb::Peer &peer) {
    raft::ConfChange cch;

    if (meta_.FindPeerByNodeID(peer.node_id())) {
        RANGE_LOG_WARN("AddPeer existed: %s", peer.ShortDebugString().c_str());
        return;
    }

    cch.type = raft::ConfChangeType::kAdd;
    cch.peer.type = peer.type() == metapb::PeerType_Learner ? raft::PeerType::kLearner
                                                            : raft::PeerType::kNormal;
    cch.peer.node_id = peer.node_id();
    cch.peer.peer_id = peer.id();

    raft_cmdpb::PeerTask pt;

    auto ap = pt.mutable_peer();
    ap->set_id(peer.id());
    ap->set_node_id(peer.node_id());
    ap->set_type(peer.type());

    auto ep = pt.mutable_verify_epoch();
    ep->set_conf_ver(meta_.GetConfVer());

    pt.SerializeToString(&cch.context);
    raft_->ChangeMemeber(cch);

    RANGE_LOG_INFO("AddPeer NodeId: %" PRIu64 " version:%" PRIu64 " conf_ver:%" PRIu64,
                   peer.node_id(), meta_.GetVersion(), meta_.GetConfVer());
}

void Range::DelPeer(const metapb::Peer &peer) {
    raft::ConfChange cch;

    metapb::Peer old_peer;
    if (!meta_.FindPeerByNodeID(peer.node_id(), &old_peer) || old_peer.id() != peer.id()) {
        RANGE_LOG_WARN("DelPeer NodeId: %" PRIu64 " peer:%" PRIu64
                  " info mismatch, Current Peer NodeId: %" PRIu64 " peer: %" PRIu64,
                  peer.node_id(), peer.id(), old_peer.node_id(), old_peer.id());
        return;
    }

    cch.type = raft::ConfChangeType::kRemove;

    cch.peer.type = peer.type() == metapb::PeerType_Learner ? raft::PeerType::kLearner
                                                            : raft::PeerType::kNormal;
    cch.peer.node_id = peer.node_id();
    cch.peer.peer_id = peer.id();
    raft_cmdpb::PeerTask pt;

    auto ap = pt.mutable_peer();
    ap->set_id(peer.id());
    ap->set_node_id(peer.node_id());
    ap->set_type(peer.type());

    auto ep = pt.mutable_verify_epoch();
    ep->set_conf_ver(meta_.GetConfVer());

    pt.SerializeToString(&cch.context);
    raft_->ChangeMemeber(cch);

    RANGE_LOG_INFO("DelPeer NodeId: %" PRIu64 " version:%" PRIu64 "conf_ver:%" PRIu64,
              peer.node_id(), meta_.GetVersion(), meta_.GetConfVer());
}

Status Range::ApplyMemberChange(const raft::ConfChange &cc, uint64_t index) {
    RANGE_LOG_INFO(
            "start ApplyMemberChange: %s, current conf ver: %" PRIu64 " at index %" PRIu64,
            cc.ToString().c_str(), meta_.GetConfVer(), index);

    Status ret;
    bool updated = false;
    switch (cc.type) {
        case raft::ConfChangeType::kAdd:
            ret = ApplyAddPeer(cc, &updated);
            break;
        case raft::ConfChangeType::kRemove:
            ret = ApplyDelPeer(cc, &updated);
            break;
        case raft::ConfChangeType::kPromote:
            ret = ApplyPromotePeer(cc, &updated);
            break;
        default:
            ret =
                Status(Status::kNotSupported, "ApplyMemberChange not support type", "");
    }

    if (!ret.ok()) {
        RANGE_LOG_ERROR("ApplyMemberChange(%s) failed: %s",
                cc.ToString().c_str(), ret.ToString().c_str());
        return ret;
    }

    if (updated) {
        ret = context_->MetaStore()->AddRange(meta_.Get());
        if (!ret.ok()) {
            RANGE_LOG_ERROR("save meta failed: %s", ret.ToString().c_str());
            return ret;
        }

        // notify master the newest peers if we are leader
        PushHeartBeatMessage();

        RANGE_LOG_INFO("ApplyMemberChange(%s) successfully.", cc.ToString().c_str());
    }

    apply_index_ = index;
    auto s = context_->MetaStore()->SaveApplyIndex(id_, apply_index_);
    if (!s.ok()) {
        RANGE_LOG_ERROR("save apply index error %s", s.ToString().c_str());
        return s;
    }
    return Status::OK();
}

Status Range::ApplyAddPeer(const raft::ConfChange &cc, bool *updated) {
    raft_cmdpb::PeerTask pt;
    auto res = common::GetMessage(cc.context.c_str(), cc.context.size(), &pt);
    if (!res) {
        return Status(Status::kInvalidArgument, "deserialize add peer context",
                      EncodeToHex(cc.context));
    }

    auto ret = meta_.AddPeer(pt.peer(), pt.verify_epoch().conf_ver());
    *updated = ret.ok();
    if (ret.code() == Status::kStaleEpoch || ret.code() == Status::kExisted) {
        RANGE_LOG_WARN("ApplyAddPeer(peer: %s) failed: %s",
                pt.peer().ShortDebugString().c_str(), ret.ToString().c_str());
        return Status::OK();
    } else {
        return ret;
    }
}

Status Range::ApplyDelPeer(const raft::ConfChange &cc, bool *updated) {
    raft_cmdpb::PeerTask pt;
    auto res = common::GetMessage(cc.context.c_str(), cc.context.size(), &pt);
    if (!res) {
        return Status(Status::kInvalidArgument, "deserialize delete peer context",
                EncodeToHex(cc.context));
    }

    auto ret = meta_.DelPeer(pt.peer(), pt.verify_epoch().conf_ver());
    *updated = ret.ok();
    if (ret.code() == Status::kStaleEpoch || ret.code() == Status::kNotFound) {
        RANGE_LOG_WARN("ApplyDelPeer(peer: %s) failed: %s",
                pt.peer().ShortDebugString().c_str(), ret.ToString().c_str());
        return Status::OK();
    } else {
        return ret;
    }
}

Status Range::ApplyPromotePeer(const raft::ConfChange &cc, bool *updated) {
    auto ret = meta_.PromotePeer(cc.peer.node_id, cc.peer.peer_id);
    *updated = ret.ok();
    if (ret.code() == Status::kNotFound) {
        RANGE_LOG_WARN("ApplyPromote(peer: %s) failed: %s",
                  cc.peer.ToString().c_str(), ret.ToString().c_str());
        return Status::OK();
    } else {
        return ret;
    }
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
