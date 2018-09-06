#include "range.h"
#include <common/ds_config.h>

#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "master/worker.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/meta_store.h"

#include "snapshot.h"
#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

static const int kDownPeerThresholdSecs = 50;
// 磁盘使用率大于百分之92停写
static const uint64_t kStopWriteFsUsagePercent = 92;

Range::Range(RangeContext* context, const metapb::Range &meta) :
	context_(context),
	node_id_(context_->GetNodeID()),
	id_(meta.id()),
	start_key_(meta.start_key()),
	meta_(meta),
	store_(new storage::Store(meta, context->DBInstance())) {
    eventBuffer = new watch::CEventBuffer(ds_config.watch_config.buffer_map_size,
                                        ds_config.watch_config.buffer_queue_size);
}


Range::~Range() {
    delete eventBuffer;
}

Status Range::Initialize(uint64_t leader, uint64_t log_start_index) {
    // 加载apply位置
    auto s = context_->MetaStore()->LoadApplyIndex(id_, &apply_index_);
    if (!s.ok()) {
        return Status(Status::kCorruption, "load applied", s.ToString());
    }

    // 创建起始日志之前的日志都算作被应用过的
    if (log_start_index > 1 && log_start_index - 1 > apply_index_) {
        apply_index_ = log_start_index - 1;
        s = context_->MetaStore()->SaveApplyIndex(id_, apply_index_);
        if (!s.ok()) {
            return Status(Status::kCorruption, "save applied", s.ToString());
        }
    }

    // 初始化raft
    raft::RaftOptions options;
    options.id = id_;
    options.leader = leader;
    options.applied = apply_index_;
    options.statemachine = shared_from_this();
    options.log_file_size = ds_config.raft_config.log_file_size;
    options.max_log_files = ds_config.raft_config.max_log_files;
    options.allow_log_corrupt = ds_config.raft_config.allow_log_corrupt > 0;
    options.initial_first_index = log_start_index;
    options.storage_path = JoinFilePath(std::vector<std::string>{
        std::string(ds_config.raft_config.log_path), std::to_string(meta_.GetTableID()),
        std::to_string(id_)});

    // init raft peers
    auto peers = meta_.GetAllPeers();
    for (const auto&peer : peers) {
        raft::Peer raft_peer;
        raft_peer.type = peer.type() == metapb::PeerType_Learner ? raft::PeerType::kLearner
                                                        : raft::PeerType::kNormal;
        raft_peer.node_id = peer.node_id();
        raft_peer.peer_id = peer.id();
        options.peers.push_back(raft_peer);
    }

    if (leader != 0) {
        options.term = 1;
    }

    // create raft group
    s = context_->RaftServer()->CreateRaft(options, &raft_);
    if (!s.ok()) {
        return Status(Status::kInvalidArgument, "create raft", s.ToString());
    }

    if (leader == node_id_) {
        context_->Statistics()->ReportLeader(id_, true);
        is_leader_ = true;
    }

    return Status::OK();
}

Status Range::Shutdown() {
    valid_ = false;

    context_->Statistics()->ReportLeader(id_, false);

    // 删除raft
    auto s = context_->RaftServer()->RemoveRaft(id_);
    if (!s.ok()) {
        RANGE_LOG_WARN("remove raft failed: %s", s.ToString().c_str());
    }
    raft_.reset();

    ClearExpiredContext();
    return Status::OK();
}

void Range::Heartbeat() {
    if (PushHeartBeatMessage()) {
        context_->ScheduleHeartbeat(id_, true);
    }

    // clear async apply expired task
    ClearExpiredContext();
}

bool Range::PushHeartBeatMessage() {
    if (!is_leader_ || !valid_ || raft_ == nullptr || raft_->IsStopped()) {
        return false;
    }

    RANGE_LOG_DEBUG(
            "heartbeat. epoch[%" PRIu64 " : %" PRIu64 "], key range[%s - %s]",
            meta_.GetVersion(), meta_.GetConfVer(), EncodeToHex(start_key_).c_str(),
            EncodeToHex(meta_.GetEndKey()).c_str());

    mspb::RangeHeartbeatRequest req;

    // 设置meta
    meta_.Get(req.mutable_range());

    // 设置leader
    auto leader_peer = req.mutable_leader();
    if (!meta_.FindPeerByNodeID(node_id_, leader_peer)) {
        RANGE_LOG_ERROR("heartbeat not found leader: %" PRIu64, node_id_);
        return false;
    }

    raft::RaftStatus rs;
    raft_->GetStatus(&rs);
    if (rs.leader != node_id_) {
        RANGE_LOG_ERROR("heartbeat raft say not leader, leader=%" PRIu64, rs.leader);
        return false;
    }
    // 设置leader term
    req.set_term(rs.term);

    for (const auto &pr : rs.replicas) {
        auto peer_status = req.add_peers_status();

        auto peer = peer_status->mutable_peer();
        if (!meta_.FindPeerByNodeID(pr.first, peer)) {
            RANGE_LOG_ERROR("heartbeat not found peer: %" PRIu64, pr.first);
            continue;
        }

        peer_status->set_index(pr.second.match);
        peer_status->set_commit(pr.second.commit);
        // 检查down peers
        if (pr.second.inactive_seconds > kDownPeerThresholdSecs) {
            peer_status->set_down_seconds(
                    static_cast<uint64_t>(pr.second.inactive_seconds));
        }
        peer_status->set_snapshotting(pr.second.snapshotting);
    }

    // metric stats
    auto stats = req.mutable_stats();
    stats->set_approximate_size(real_size_);

    storage::MetricStat store_stat;
    store_->CollectMetric(&store_stat);
    stats->set_keys_read(store_stat.keys_read_per_sec);
    stats->set_bytes_read(store_stat.bytes_read_per_sec);
    stats->set_keys_written(store_stat.keys_write_per_sec);
    stats->set_bytes_written(store_stat.bytes_write_per_sec);

    context_->MasterClient()->AsyncRangeHeartbeat(req);

    return true;
}

Status Range::Apply(const raft_cmdpb::Command &cmd, uint64_t index) {
    if (!CheckWriteable()) {
        return Status(Status::kIOError, "no left space", "apply");
    }

    switch (cmd.cmd_type()) {
        case raft_cmdpb::CmdType::Lock:
            return ApplyLock(cmd);
        case raft_cmdpb::CmdType::LockUpdate:
            return ApplyLockUpdate(cmd);
        case raft_cmdpb::CmdType::Unlock:
            return ApplyUnlock(cmd);
        case raft_cmdpb::CmdType::UnlockForce:
            return ApplyUnlockForce(cmd);

        case raft_cmdpb::CmdType::RawPut:
            return ApplyRawPut(cmd);
        case raft_cmdpb::CmdType::RawDelete:
            return ApplyRawDelete(cmd);
        case raft_cmdpb::CmdType::Insert:
            return ApplyInsert(cmd);
        case raft_cmdpb::CmdType::Delete:
            return ApplyDelete(cmd);
        case raft_cmdpb::CmdType::KvSet:
            return ApplyKVSet(cmd);
        case raft_cmdpb::CmdType::KvBatchSet:
            return ApplyKVBatchSet(cmd);
        case raft_cmdpb::CmdType::KvDelete:
            return ApplyKVDelete(cmd);
        case raft_cmdpb::CmdType::KvBatchDel:
            return ApplyKVBatchDelete(cmd);
        case raft_cmdpb::CmdType::KvRangeDel:
            return ApplyKVRangeDelete(cmd);
        case raft_cmdpb::CmdType::KvWatchPut:
            return ApplyWatchPut(cmd, index);
        case raft_cmdpb::CmdType::KvWatchDel:
            return ApplyWatchDel(cmd, index);
        default:
            RANGE_LOG_ERROR("Apply cmd type error %s", CmdType_Name(cmd.cmd_type()).c_str());
            return Status(Status::kNotSupported, "cmd type not supported", "");
    }
}

Status Range::Apply(const std::string &cmd, uint64_t index) {
    if (!valid_) {
        RANGE_LOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    auto start = std::chrono::system_clock::now();

    raft_cmdpb::Command raft_cmd;
    common::GetMessage(cmd.data(), cmd.size(), &raft_cmd);

    Status ret;
    if (raft_cmd.cmd_type() == raft_cmdpb::CmdType::AdminSplit) {
        ret = ApplySplit(raft_cmd, index);
    } else {
        auto ret = Apply(raft_cmd, index);
        // 非IO错误(致命），不给raft返回错误，不然raft会停止自己
        if (!ret.ok() && ret.code() != Status::kIOError) {
            ret = Status::OK();
        }
    }
    if (!ret.ok()) {
        return ret;
    }

    apply_index_ = index;
    auto s = context_->MetaStore()->SaveApplyIndex(id_, apply_index_);
    if (!s.ok()) {
        RANGE_LOG_ERROR("save apply index error %s", s.ToString().c_str());
        return s;
    }

    auto end = std::chrono::system_clock::now();
    auto elapsed_usec =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    if (elapsed_usec > kTimeTakeWarnThresoldUSec) {
        RANGE_LOG_WARN("apply takes too long(%ld ms), type: %s.", elapsed_usec / 1000,
                  raft_cmdpb::CmdType_Name(raft_cmd.cmd_type()).c_str());
    }

    return Status::OK();
}

Status Range::Submit(const raft_cmdpb::Command &cmd) {
    if (is_leader_) {
        std::string str_cmd = std::move(cmd.SerializeAsString());
        if (str_cmd.empty()) {
            return Status(Status::kCorruption, "protobuf serialize failed", "");
        }
        return raft_->Submit(str_cmd);
        // return Apply(cmd,0);
    } else {
        return Status(Status::kNotLeader, "Not Leader", "");
    }
}

Status Range::SubmitCmd(common::ProtoMessage *msg, const kvrpcpb::RequestHeader& header,
                 const std::function<void(raft_cmdpb::Command &cmd)> &init) {
    raft_cmdpb::Command cmd;
    init(cmd);

    // set verify epoch
    auto epoch = new metapb::RangeEpoch(header.range_epoch());
    cmd.set_allocated_verify_epoch(epoch);

    // add to queue
    auto seq = submit_queue_.Add(header, cmd.cmd_type(), msg);
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(seq);

    auto ret = Submit(cmd);
    if (!ret.ok()) {
        submit_queue_.Remove(cmd.cmd_id().seq());
    }

    return ret;
}

void Range::OnLeaderChange(uint64_t leader, uint64_t term) {
    RANGE_LOG_INFO("Leader Change to Node %" PRIu64, leader);

    if (!valid_) {
        RANGE_LOG_ERROR("is invalid!");
        return;
    }

    bool prev_is_leader = is_leader_;
    is_leader_ = (leader == node_id_);
    if (is_leader_) {
        if (!prev_is_leader) {
            store_->ResetMetric();
        }
        context_->ScheduleHeartbeat(id_, false);
    }
    context_->Statistics()->ReportLeader(id_, is_leader_.load());
}

std::shared_ptr<raft::Snapshot> Range::GetSnapshot() {
    raft_cmdpb::SnapshotContext ctx;
    meta_.Get(ctx.mutable_meta());
    return std::shared_ptr<raft::Snapshot>(
        new Snapshot(apply_index_, std::move(ctx), store_->NewIterator()));
}

Status Range::ApplySnapshotStart(const std::string &context) {
    RANGE_LOG_INFO("apply snapshot begin");

    if (!valid_) {
        RANGE_LOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    apply_index_ = 0;

    auto s = store_->Truncate();
    if (!s.ok()) {
        return s;
    }

    raft_cmdpb::SnapshotContext ctx;
    if (!ctx.ParseFromString(context)) {
        return Status(Status::kCorruption, "parse snapshot context", "pb return false");
    }

    meta_.Set(ctx.meta());
    s = SaveMeta(ctx.meta()) ;
    if (!s.ok()) {
        RANGE_LOG_ERROR("save snapshot meta failed: %s", s.ToString().c_str());
        return Status(Status::kIOError, "save range meta", "");
    }

    RANGE_LOG_INFO("meta update to %s", ctx.meta().ShortDebugString().c_str());

    return Status::OK();
}

Status Range::ApplySnapshotData(const std::vector<std::string> &datas) {
    if (!valid_) {
        RANGE_LOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    return store_->ApplySnapshot(datas);
}

Status Range::ApplySnapshotFinish(uint64_t index) {
    RANGE_LOG_INFO("finish apply snapshot. index: %" PRIu64, index);

    if (!valid_) {
        RANGE_LOG_ERROR("is invalid!");
        return Status(Status::kInvalid, "range is invalid", "");
    }

    apply_index_ = index;
    auto s = context_->MetaStore()->SaveApplyIndex(id_, index);
    if (!s.ok()) {
        RANGE_LOG_ERROR("save snapshot applied index failed(%s)!", s.ToString().c_str());
        return s;
    } else {
        return Status::OK();
    }
}

Status Range::SaveMeta(const metapb::Range &meta) {
    return context_->MetaStore()->AddRange(meta);
}

Status Range::Destroy() {
    valid_ = false;

    ClearExpiredContext();

    context_->Statistics()->ReportLeader(id_, false);

    // 销毁raft
    auto s = context_->RaftServer()->DestroyRaft(id_);
    if (!s.ok()) {
        RANGE_LOG_WARN("destroy raft failed: %s", s.ToString().c_str());
    }
    raft_.reset();

    s = store_->Truncate();
    if (!s.ok()) {
        RANGE_LOG_ERROR("truncate store fail: %s", s.ToString().c_str());
        return s;
    }
    s = context_->MetaStore()->DeleteApplyIndex(id_);
    if (!s.ok()) {
        RANGE_LOG_ERROR("truncate delete apply fail: %s", s.ToString().c_str());
    }
    return s;
}

void Range::TransferLeader() {
    if (!valid_) {
        RANGE_LOG_ERROR("is invalid!");
        return;
    }

    RANGE_LOG_INFO("receive TransferLeader, try to leader.");

    auto s = raft_->TryToLeader();
    if (!s.ok()) {
        RANGE_LOG_ERROR("TransferLeader fail, %s", s.ToString().c_str());
    }
}

void Range::GetPeerInfo(raft::RaftStatus *raft_status) { raft_->GetStatus(raft_status); }

void Range::GetReplica(metapb::Replica *rep) {
    rep->set_range_id(id_);
    rep->set_start_key(start_key_);
    rep->set_end_key(meta_.GetEndKey());
    auto peer = new metapb::Peer;
    meta_.FindPeerByNodeID(node_id_, peer);
    rep->set_allocated_peer(peer);
}

bool Range::VerifyLeader(errorpb::Error *&err) {
    uint64_t leader, term;
    raft_->GetLeaderTerm(&leader, &term);

    // we are leader
    if (leader == node_id_) return true;

    metapb::Peer peer;
    if (leader == 0 || !meta_.FindPeerByNodeID(leader, &peer)) {
        err = NoLeaderError();
    } else {
        err = NotLeaderError(std::move(peer));
    }
    return false;
}

bool Range::VerifyReadable(uint64_t read_index, errorpb::Error *&err) {
    uint64_t leader, term;
    raft_->GetLeaderTerm(&leader, &term);
    // we are leader
    if (leader == node_id_) {
        return true;
    } else if (read_index != 0) {
        auto current_index = apply_index_;
        if (read_index > current_index) {
            err = StaleReadIndexError(read_index, current_index);
            return false;
        } else {
            return true;
        }
    } else {
        metapb::Peer peer;
        if (leader == 0 || !meta_.FindPeerByNodeID(leader, &peer)) {
            err = NoLeaderError();
        } else {
            err = NotLeaderError(std::move(peer));
        }
        return false;
    }
}

bool Range::CheckWriteable() {
    auto percent = context_->GetFSUsagePercent();
    if (percent > kStopWriteFsUsagePercent) {
        RANGE_LOG_ERROR(
                "filesystem usage percent(%" PRIu64 "> %" PRIu64 ") limit reached, reject write request",
                percent, kStopWriteFsUsagePercent);
        return false;
    } else {
        return true;
    }
}

bool Range::KeyInRange(const std::string &key) {
    if (key < start_key_) {
        RANGE_LOG_WARN("key: %s less than start_key:%s, out of range",
                  EncodeToHex(key).c_str(), EncodeToHex(start_key_).c_str());
        return false;
    }

    auto end_key = meta_.GetEndKey();
    if (key >= end_key) {
        RANGE_LOG_WARN("key: %s greater than end_key:%s, out of range",
                  EncodeToHex(key).c_str(), EncodeToHex(end_key).c_str());
        return false;
    }

    return true;
}

bool Range::KeyInRange(const std::string &key, errorpb::Error *&err) {
    if (!KeyInRange(key)) {
        err = KeyNotInRange(key);
        return false;
    }

    return true;
}

bool Range::EpochIsEqual(const metapb::RangeEpoch &epoch) {
    return meta_.GetVersion() == epoch.version();
}

bool Range::EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&err) {
    if (!EpochIsEqual(epoch)) {
        err = StaleEpochError(epoch);
        return false;
    }
    return true;
}

void Range::ClearExpiredContext() {
    auto expired_seqs = submit_queue_.GetExpired();
    for (auto seq: expired_seqs) {
        auto ctx = submit_queue_.Remove(seq);
        if (ctx) {
            ctx->SendTimeout(context_->SocketSession());
        }
    }
}


errorpb::Error *Range::NoLeaderError() {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("no leader");
    err->mutable_not_leader()->set_range_id(id_);
    meta_.GetEpoch(err->mutable_not_leader()->mutable_epoch());

    return err;
}

errorpb::Error *Range::NotLeaderError(metapb::Peer &&peer) {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("not leader");
    err->mutable_not_leader()->set_range_id(id_);
    err->mutable_not_leader()->set_allocated_leader(new metapb::Peer(std::move(peer)));
    meta_.GetEpoch(err->mutable_not_leader()->mutable_epoch());

    return err;
}

errorpb::Error *Range::KeyNotInRange(const std::string &key) {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("key not in range");
    err->mutable_key_not_in_range()->set_range_id(id_);
    err->mutable_key_not_in_range()->set_key(key);
    err->mutable_key_not_in_range()->set_start_key(start_key_);

    // end_key change at range split time
    err->mutable_key_not_in_range()->set_end_key(meta_.GetEndKey());

    return err;
}

errorpb::Error *Range::RaftFailError() {
    errorpb::Error *err = new errorpb::Error;
    err->set_message("raft submit fail");
    err->mutable_raft_fail();
    return err;
}

errorpb::Error *Range::StaleEpochError(const metapb::RangeEpoch &epoch) {
    errorpb::Error *err = new errorpb::Error;
    std::string msg = "stale epoch, req version:";
    msg += std::to_string(epoch.version());
    msg += " cur version:";
    msg += std::to_string(meta_.GetVersion());

    err->set_message(std::move(msg));
    auto stale_epoch = err->mutable_stale_epoch();
    meta_.Get(stale_epoch->mutable_old_range());

    if (split_range_id_ > 0) {
        auto split_range = context_->FindRange(split_range_id_);
        if (split_range) {
            auto split_meta = split_range->options();
            stale_epoch->set_allocated_new_range(new metapb::Range(std::move(split_meta)));
        }
    }

    return err;
}

errorpb::Error *Range::StaleReadIndexError(uint64_t read_index, uint64_t current_index) {
    errorpb::Error *err = new errorpb::Error;
    err->mutable_stale_read_index()->set_read_index(read_index);
    err->mutable_stale_read_index()->set_replica_index(current_index);
    return err;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
