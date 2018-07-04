#include "range.h"
#include <common/ds_config.h>

#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "master/worker.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/meta_store.h"

#include "snapshot.h"

namespace sharkstore {
namespace dataserver {
namespace range {

static const int kDownPeerThresholdSecs = 50;
// 磁盘使用率大于百分之92停写
static const uint64_t kStopWriteFsUsagePercent = 92;

Range::Range(server::ContextServer *context, const metapb::Range &meta)
    : context_(context), meta_(meta) {
    node_id_ = context->node_id;

    store_ = new storage::Store(meta, context->rocks_db);
}

Range::~Range() { delete store_; }

Status Range::Initialize(range_status_t *status, uint64_t leader) {
    // 加载apply位置
    auto s = context_->meta_store->LoadApplyIndex(meta_.id(), &apply_index_);
    if (!s.ok()) {
        return Status(Status::kCorruption, "load applied", s.ToString());
    }

    // 初始化raft
    raft::RaftOptions options;
    options.id = meta_.id();
    options.leader = leader;
    options.applied = apply_index_;
    options.statemachine = shared_from_this();
    options.log_file_size = ds_config.raft_config.log_file_size;
    options.max_log_files = ds_config.raft_config.max_log_files;
    options.allow_log_corrupt = ds_config.raft_config.allow_log_corrupt;
    options.storage_path = JoinFilePath(std::vector<std::string>{
        std::string(ds_config.raft_config.log_path), std::to_string(meta_.table_id()),
        std::to_string(meta_.id())});
    // meta_.peers
    // 有可能有多个，如果该range的副本数量在master上有多个，通过该字段下发已经有的副本
    const auto &peers = meta_.peers();
    for (auto it = peers.begin(); it != peers.end(); ++it) {
        raft::Peer p;
        p.type = it->type() == metapb::PeerType_Learner ? raft::PeerType::kLearner
                                                        : raft::PeerType::kNormal;
        p.node_id = it->node_id();
        p.peer_id = it->id();
        options.peers.push_back(p);
    }

    if (leader != 0) {
        options.term = 1;
    }

    range_status_ = status;
    // create raft group
    s = context_->raft_server->CreateRaft(options, &raft_);
    if (!s.ok()) {
        return Status(Status::kInvalidArgument, "create raft", s.ToString());
    }

    if (leader == node_id_) {
        is_leader_ = true;
    }

    return Status::OK();
}

Status Range::Shutdown() {
    valid_ = false;

    // 删除raft
    auto s = context_->raft_server->RemoveRaft(meta_.id());
    if (!s.ok()) {
        FLOG_WARN("range[%" PRIu64 "] remove raft failed: %s", meta_.id(),
                  s.ToString().c_str());
    }

    raft_.reset();

    ClearExpiredContext();
    return Status::OK();
}

void Range::Heartbeat() {
    if (PushHeartBeatMessage()) {
        context_->range_server->LeaderQueuePush(
            meta_.id(), ds_config.hb_config.range_interval * 1000 + getticks());
    }

    // clear async apply expired task
    ClearExpiredContext();
}

bool Range::PushHeartBeatMessage() {
    if (!is_leader_ || !valid_ || raft_ == nullptr || raft_->IsStopped()) {
        return false;
    }

    FLOG_DEBUG("range[%" PRIu64 "] heartbeat. epoch[%" PRIu64 " : %" PRIu64
               "], key range[%s - %s]",
               meta_.id(), meta_.range_epoch().version(), meta_.range_epoch().conf_ver(),
               EncodeToHexString(meta_.start_key()).c_str(),
               EncodeToHexString(meta_.end_key()).c_str());

    mspb::RangeHeartbeatRequest req;

    // 设置meta
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);
        req.set_allocated_range(new metapb::Range(meta_));
    }

    // 设置leader
    auto leader_peer = req.mutable_leader();
    if (!FindPeerByNodeID(node_id_, leader_peer)) {
        FLOG_ERROR("range[%" PRIu64 "] heartbeat not found leader: %" PRIu64, meta_.id(),
                   node_id_);
        return false;
    }

    raft::RaftStatus rs;
    raft_->GetStatus(&rs);
    if (rs.leader != node_id_) {
        FLOG_ERROR("range[%" PRIu64 "] heartbeat raft say not leader, leader=%" PRIu64,
                   meta_.id(), rs.leader);
        return false;
    }
    // 设置leader term
    req.set_term(rs.term);

    for (const auto &pr : rs.replicas) {
        auto peer_status = req.add_peers_status();

        auto peer = peer_status->mutable_peer();
        if (!FindPeerByNodeID(pr.first, peer)) {
            FLOG_ERROR("range[%" PRIu64 "] heartbeat not found peer: %" PRIu64,
                       meta_.id(), pr.first);
            continue;
        }

        peer_status->set_index(pr.second.match);
        peer_status->set_commit(pr.second.commit);
        // 检查down peers
        if (pr.second.inactive_seconds > kDownPeerThresholdSecs) {
            peer_status->set_down_seconds(pr.second.inactive_seconds);
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

    context_->master_worker->AsyncRangeHeartbeat(req);

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
        default:
            FLOG_ERROR("range[%" PRIu64 "] Apply cmd type error %s", meta_.id(),
                       CmdType_Name(cmd.cmd_type()).c_str());
            return Status(Status::kNotSupported, "cmd type not supported", "");
    }
}

Status Range::Apply(const std::string &cmd, uint64_t index) {
    auto start = std::chrono::system_clock::now();

    raft_cmdpb::Command raft_cmd;
    context_->socket_session->GetMessage(cmd.data(), cmd.size(), &raft_cmd);

    if (!valid_) {
        FLOG_ERROR("range[%" PRIu64 "] is invalid!", meta_.id());
        DelContext(raft_cmd.cmd_id().seq());
        return Status(Status::kInvalid, "range is invalid", "");
    }

    Status ret;
    if (raft_cmd.cmd_type() == raft_cmdpb::CmdType::AdminSplit) {
        ret = ApplySplit(raft_cmd);
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
    auto s = context_->meta_store->SaveApplyIndex(meta_.id(), apply_index_);
    if (!s.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] save apply index error %s", meta_.id(),
                   s.ToString().c_str());
        return s;
    }

    auto end = std::chrono::system_clock::now();
    auto elapsed_usec =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    if (elapsed_usec > kTimeTakeWarnThresoldUSec) {
        FLOG_WARN("range[%lu] apply takes too long(%ld ms), type: %s.", meta_.id(),
                  elapsed_usec / 1000,
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

void Range::OnLeaderChange(uint64_t leader, uint64_t term) {
    FLOG_INFO("range[%" PRIu64 "] Leader Change to Node %" PRIu64, meta_.id(), leader);

    if (!valid_) {
        FLOG_ERROR("range[%" PRIu64 "] is invalid!", meta_.id());
        return;
    }

    if (leader == node_id_) {
        if (!is_leader_) {  // check leader to leader
            is_leader_ = true;

            store_->ResetMetric();

            context_->range_server->LeaderQueuePush(meta_.id(), getticks());
            range_status_->range_leader_count++;

            context_->run_status->PushRange(monitor::RangeTag::LeaderCount,
                                            range_status_->range_leader_count);
        }

    } else {
        if (is_leader_) {
            is_leader_ = false;
            range_status_->range_leader_count--;

            context_->run_status->PushRange(monitor::RangeTag::LeaderCount,
                                            range_status_->range_leader_count);
        }
    }
}

std::shared_ptr<raft::Snapshot> Range::GetSnapshot() {
    raft_cmdpb::SnapshotContext ctx;
    auto meta = ctx.mutable_meta();
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);
        meta->CopyFrom(meta_);
    }

    return std::shared_ptr<raft::Snapshot>(
        new Snapshot(apply_index_, std::move(ctx), store_->NewIterator()));
}

Status Range::ApplySnapshotStart(const std::string &context) {
    FLOG_INFO("Range %" PRIu64 " apply snapshot begin", meta_.id());

    if (!valid_) {
        FLOG_ERROR("range[%" PRIu64 "] is invalid!", meta_.id());
        return Status(Status::kInvalid, "range is invalid", "");
    }

    auto s = store_->Truncate();
    if (!s.ok()) {
        return s;
    }

    raft_cmdpb::SnapshotContext ctx;
    if (!ctx.ParseFromString(context)) {
        return Status(Status::kCorruption, "parse snapshot context", "pb return false");
    }

    {
        std::unique_lock<sharkstore::shared_mutex> lock(meta_lock_);
        meta_ = ctx.meta();
    }

    if (!SaveMeta(ctx.meta())) {
        return Status(Status::kIOError, "save range meta", "");
    }
    return Status::OK();
}

Status Range::ApplySnapshotData(const std::vector<std::string> &datas) {
    if (!valid_) {
        FLOG_ERROR("range[%" PRIu64 "] is invalid!", meta_.id());
        return Status(Status::kInvalid, "range is invalid", "");
    }

    return store_->ApplySnapshot(datas);
}

Status Range::ApplySnapshotFinish(uint64_t index) {
    FLOG_INFO("Range %" PRIu64 " finish apply snapshot. index:%lu", meta_.id(), index);

    if (!valid_) {
        FLOG_ERROR("range[%" PRIu64 "] is invalid!", meta_.id());
        return Status(Status::kInvalid, "range is invalid", "");
    }

    apply_index_ = index;
    auto s = context_->meta_store->SaveApplyIndex(meta_.id(), index);
    if (!s.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] save snapshot applied index failed(%s)!",
                   meta_.id(), s.ToString().c_str());
        return s;
    } else {
        return Status::OK();
    }
}

bool Range::SaveMeta(const metapb::Range &meta) {
    std::string value;
    if (!meta.SerializeToString(&value)) {
        FLOG_ERROR("save range mate seriaize failed");
        return false;
    }

    auto ms = context_->range_server->meta_store();
    auto ret = ms->AddRange(meta_.id(), value);
    if (!ret.ok()) {
        FLOG_ERROR("save range meta failed");
        return false;
    }

    return true;
}

Status Range::Truncate() {
    auto s = store_->Truncate();
    if (!s.ok()) {
        FLOG_ERROR("Range %" PRIu64 " truncate store fail: %s", meta_.id(),
                   s.ToString().c_str());
        return s;
    }
    s = context_->meta_store->DeleteApplyIndex(meta_.id());
    if (!s.ok()) {
        FLOG_ERROR("Range %" PRIu64 " truncate delete apply fail: %s", meta_.id(),
                   s.ToString().c_str());
    }
    return s;
}

void Range::TransferLeader() {
    if (!valid_) {
        FLOG_ERROR("range[%" PRIu64 "] is invalid!", meta_.id());
        return;
    }

    FLOG_INFO("range[%" PRIu64 "] receive TransferLeader, try to leader.", meta_.id());

    auto s = raft_->TryToLeader();
    if (!s.ok()) {
        FLOG_ERROR("Range %" PRIu64 " TransferLeader fail, %s", meta_.id(),
                   s.ToString().c_str());
    }
}

void Range::GetPeerInfo(raft::RaftStatus *raft_status) { raft_->GetStatus(raft_status); }

void Range::GetReplica(metapb::Replica *rep) {
    rep->set_range_id(meta_.id());
    rep->set_start_key(meta_.start_key());
    rep->set_end_key(meta_.end_key());
    auto peer = new metapb::Peer;
    FindPeerByNodeID(node_id_, peer);
    rep->set_allocated_peer(peer);
}

bool Range::VerifyLeader(errorpb::Error *&err) {
    uint64_t leader, term;
    raft_->GetLeaderTerm(&leader, &term);

    // we are leader
    if (leader == node_id_) return true;

    metapb::Peer peer;
    if (leader == 0 || !FindPeerByNodeID(leader, &peer)) {
        err = NoLeaderError();
    } else {
        err = NotLeaderError(std::move(peer));
    }
    return false;
}

bool Range::CheckWriteable() {
    if (server::RunStatus::fs_usage_percent_ >= kStopWriteFsUsagePercent) {
        FLOG_ERROR("range[%lu] filesystem usage percent(%lu > %lu) limit "
                   "reached, reject write request",
                   meta_.id(), server::RunStatus::fs_usage_percent_.load(),
                   kStopWriteFsUsagePercent);
        return false;
    } else {
        return true;
    }
}

bool Range::KeyInRange(const std::string &key) {
    if (key < meta_.start_key()) {
        FLOG_WARN("key: %s less than start_key:%s, out of range %" PRIu64,
                  EncodeToHexString(key).c_str(),
                  EncodeToHexString(meta_.start_key()).c_str(), meta_.id());
        return false;
    }

    sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);
    if (key >= meta_.end_key()) {
        FLOG_WARN("key: %s greater than end_key:%s, out of range %" PRIu64,
                  EncodeToHexString(key).c_str(),
                  EncodeToHexString(meta_.end_key()).c_str(), meta_.id());
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
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);

    if (meta_.range_epoch().version() == epoch.version()) {
        return true;
    }

    return false;
}

bool Range::EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&err) {
    if (!EpochIsEqual(epoch)) {
        err = StaleEpochError(epoch);
        return false;
    }

    return true;
}

Range::AsyncContext *Range::AddContext(uint64_t id, raft_cmdpb::CmdType type,
                                       common::ProtoMessage *msg,
                                       kvrpcpb::RequestHeader *req) {
    AsyncContext *context = new AsyncContext(type, context_, msg, req);

    std::lock_guard<std::mutex> lock(submit_mutex_);
    submit_map_[id] = context;
    submit_queue_.emplace(msg->expire_time, id);
    return context;
}

void Range::DelContext(uint64_t seq_id) {
    std::lock_guard<std::mutex> lock(submit_mutex_);
    auto it = submit_map_.find(seq_id);
    if (it != submit_map_.end()) {
        delete it->second;
        submit_map_.erase(it);
    }
}

Range::AsyncContext *Range::ReleaseContext(uint64_t seq_id) {
    std::lock_guard<std::mutex> lock(submit_mutex_);
    auto it = submit_map_.find(seq_id);
    if (it != submit_map_.end()) {
        auto context = it->second;
        submit_map_.erase(it);
        return context;
    }

    return nullptr;
}

std::tuple<bool, uint64_t> Range::GetExpiredContext() {
    std::lock_guard<std::mutex> lock(submit_mutex_);
    if (submit_queue_.empty()) {
        return std::make_tuple(true, 0);
    }
    auto ts = submit_queue_.top();
    if (valid_) {
        time_t now = getticks();
        if (ts.first > now) {
            return std::make_tuple(false, 0);
        }
    }

    submit_queue_.pop();
    return std::make_tuple(false, ts.second);
}

void Range::ClearExpiredContext() {
    bool empty = false;
    uint64_t seq_id = -1;

    while (true) {
        do {
            std::tie(empty, seq_id) = GetExpiredContext();
            if (empty || seq_id == 0) {
                break;
            }

            Range::AsyncContext *context = ReleaseContext(seq_id);
            if (context != nullptr) {
                SendTimeOutError(context);
            }

        } while (seq_id > 0);

        if (empty || is_leader_) {
            break;
        }
    }
}

errorpb::Error *Range::NoLeaderError() {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("no leader");
    err->mutable_not_leader()->set_range_id(meta_.id());
    err->mutable_not_leader()->set_allocated_epoch(
        new metapb::RangeEpoch(meta_.range_epoch()));

    return err;
}

errorpb::Error *Range::NotLeaderError(metapb::Peer &&peer) {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("not leader");
    err->mutable_not_leader()->set_range_id(meta_.id());
    err->mutable_not_leader()->set_allocated_epoch(
        new metapb::RangeEpoch(meta_.range_epoch()));
    err->mutable_not_leader()->set_allocated_leader(new metapb::Peer(std::move(peer)));

    return err;
}

errorpb::Error *Range::KeyNotInRange(const std::string &key) {
    errorpb::Error *err = new errorpb::Error;

    err->set_message("key not in range");
    err->mutable_key_not_in_range()->set_range_id(meta_.id());
    err->mutable_key_not_in_range()->set_key(key);
    err->mutable_key_not_in_range()->set_start_key(meta_.start_key());

    // end_key change at range split time
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(meta_lock_);
    err->mutable_key_not_in_range()->set_end_key(meta_.end_key());

    return err;
}

errorpb::Error *Range::TimeOutError() {
    errorpb::Error *err = new errorpb::Error;
    err->set_message("requset timeout");
    err->mutable_timeout();
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
    msg += std::to_string(meta_.range_epoch().version());

    err->set_message(std::move(msg));
    auto stale_epoch = err->mutable_stale_epoch();
    stale_epoch->set_allocated_old_range(new metapb::Range(meta_));

    if (split_range_id_ > 0) {
        auto new_meta = context_->range_server->GetRangeMeta(split_range_id_);
        if (new_meta != nullptr) {
            stale_epoch->set_allocated_new_range(new_meta);
        }
    }

    return err;
}

void Range::SendTimeOutError(AsyncContext *context) {
    FLOG_WARN("range[%lu] deal %s timeout. sid=%ld, msgid=%ld", meta_.id(),
              raft_cmdpb::CmdType_Name(context->cmd_type_).c_str(),
              context->proto_message->session_id, context->proto_message->msg_id);

    auto err = TimeOutError();
    switch (context->cmd_type_) {
        case raft_cmdpb::CmdType::RawPut:
            SendError(context, new kvrpcpb::DsKvRawPutResponse, err);
            break;
        case raft_cmdpb::CmdType::RawDelete:
            SendError(context, new kvrpcpb::DsKvRawDeleteResponse, err);
            break;
        case raft_cmdpb::CmdType::Insert:
            SendError(context, new kvrpcpb::DsInsertResponse, err);
            break;
        case raft_cmdpb::CmdType::Delete:
            SendError(context, new kvrpcpb::DsDeleteResponse, err);
            break;

        default:
            FLOG_ERROR("range[%" PRIu64 "] Apply cmd type error %d", meta_.id(),
                       context->cmd_type_);
            delete err;
    }

    delete context;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
