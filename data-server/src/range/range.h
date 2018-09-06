_Pragma("once");

#include <stdint.h>
#include <atomic>
#include <string>

#include "frame/sf_logger.h"
#include "frame/sf_util.h"

#include "base/shared_mutex.h"
#include "base/util.h"

#include "common/generater.h"
#include "common/ds_encoding.h"
#include "common/socket_session.h"

#include "storage/store.h"

#include "raft/raft.h"
#include "raft/statemachine.h"
#include "raft/types.h"

#include "proto/gen/funcpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/watchpb.pb.h"

#include "server/context_server.h"
#include "server/run_status.h"
#include "watch/watch_event_buffer.h"
#include "watch/watcher.h"

#include "meta_keeper.h"
#include "context.h"
#include "submit.h"
#include "range_logger.h"

// for test friend class
namespace sharkstore { namespace test { namespace helper { class RangeTestFixture; }}}

namespace sharkstore {
namespace dataserver {
namespace range {

const int DEFAULT_LOCK_DELETE_TIME_MILLSEC = 3000;
enum {
    LOCK_OK = 0,
    LOCK_NOT_EXIST = Status::kNotFound,
    LOCK_EXISTED,
    LOCK_ID_MISMATCHED,
    LOCK_IS_FORCE_UNLOCKED,
    LOCK_STORE_FAILED,
    LOCK_EPOCH_ERROR,
    LOCK_TIME_OUT = Status::kTimedOut,  //value 7 same with defined in status.h
    LOCK_PARAMETER_ERROR
};

class Range : public raft::StateMachine, public std::enable_shared_from_this<Range> {
public:
    Range(RangeContext* context, const metapb::Range &meta);
    ~Range();

    Range(const Range &) = delete;
    Range &operator=(const Range &) = delete;
    Range &operator=(const Range &) volatile = delete;

    Status Initialize(uint64_t leader = 0, uint64_t log_start_index = 0);
    Status Shutdown();

    Status Apply(const std::string &cmd, uint64_t index) override;
    Status ApplyMemberChange(const raft::ConfChange &cc, uint64_t index) override;

    void OnReplicateError(const std::string &cmd, const Status &status) override {};

    void OnLeaderChange(uint64_t leader, uint64_t term) override;

    std::shared_ptr<raft::Snapshot> GetSnapshot() override;
    Status ApplySnapshotStart(const std::string &context) override;
    Status ApplySnapshotData(const std::vector<std::string> &datas) override;
    Status ApplySnapshotFinish(uint64_t index) override;

    void TransferLeader();
    void GetPeerInfo(raft::RaftStatus *raft_status);
    uint64_t GetPeerID() const;

    Status ForceSplit(uint64_t version, std::string* split_key);

    // lock
    kvrpcpb::LockValue *LockGet(const std::string &key);
    void Lock(common::ProtoMessage *msg, kvrpcpb::DsLockRequest &req);
    void LockUpdate(common::ProtoMessage *msg, kvrpcpb::DsLockUpdateRequest &req);
    void Unlock(common::ProtoMessage *msg, kvrpcpb::DsUnlockRequest &req);
    void UnlockForce(common::ProtoMessage *msg, kvrpcpb::DsUnlockForceRequest &req);
    void LockWatch(common::ProtoMessage *msg, watchpb::DsWatchRequest& req);
    void LockScan(common::ProtoMessage *msg, kvrpcpb::DsLockScanRequest &req);

    // KV
    void RawGet(common::ProtoMessage *msg, kvrpcpb::DsKvRawGetRequest &req);
    void RawPut(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req);
    void RawDelete(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req);

    void Insert(common::ProtoMessage *msg, kvrpcpb::DsInsertRequest &req);
    void Select(common::ProtoMessage *msg, kvrpcpb::DsSelectRequest &req);
    void Delete(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req);
    
    void KVSet(common::ProtoMessage *msg, kvrpcpb::DsKvSetRequest &req);
    void KVGet(common::ProtoMessage *msg, kvrpcpb::DsKvGetRequest &req);
    void KVBatchSet(common::ProtoMessage *msg, kvrpcpb::DsKvBatchSetRequest &req);
    void KVBatchGet(common::ProtoMessage *msg, kvrpcpb::DsKvBatchGetRequest &req);
    void KVDelete(common::ProtoMessage *msg, kvrpcpb::DsKvDeleteRequest &req);
    void KVBatchDelete(common::ProtoMessage *msg, kvrpcpb::DsKvBatchDeleteRequest &req);
    void KVRangeDelete(common::ProtoMessage *msg, kvrpcpb::DsKvRangeDeleteRequest &req);
    void KVScan(common::ProtoMessage *msg, kvrpcpb::DsKvScanRequest &req);

    //KV watch series
    Status GetAndResp( watch::WatcherPtr pWatcher, const watchpb::WatchCreateRequest& req, const std::string &dbKey, const bool &prefix,
                              int64_t &version, watchpb::DsWatchResponse *dsResp);
    void WatchGet(common::ProtoMessage *msg, watchpb::DsWatchRequest &req);
    void PureGet(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req);
    void WatchPut(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req);
    void WatchDel(common::ProtoMessage *msg, watchpb::DsKvWatchDeleteRequest &req);
    bool WatchPutSubmit(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req);
    bool WatchDeleteSubmit(common::ProtoMessage *msg,
                            watchpb::DsKvWatchDeleteRequest &req);

    watch::CEventBuffer *getEventBuffer() {
        return  eventBuffer;
    }
public:
    kvrpcpb::KvRawGetResponse *RawGetResp(const std::string &key);
    kvrpcpb::SelectResponse *SelectResp(const kvrpcpb::DsSelectRequest &req);
    // RawPutSubmit cannot be called repeatedly
    bool RawPutSubmit(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req);
    // RawDeleteSubmit cannot be called repeatedly
    bool RawDeleteSubmit(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req);
    // DeleteSubmit cannot be called repeatedly
    bool DeleteSubmit(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req);

private:
    void ClearExpiredContext();

private:
    kvrpcpb::KvRawGetResponse *RawGetTry(const std::string &key);
    kvrpcpb::SelectResponse *SelectTry(const kvrpcpb::DsSelectRequest &req);
    bool RawPutTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req);
    bool RawDeleteTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req);
    bool DeleteTry(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req);
    
private:
    Status Submit(const raft_cmdpb::Command &cmd);

    Status SubmitCmd(common::ProtoMessage *msg, const kvrpcpb::RequestHeader& header,
                     const std::function<void(raft_cmdpb::Command &cmd)> &init);

    Status Apply(const raft_cmdpb::Command &cmd, uint64_t index);

    Status ApplyRawPut(const raft_cmdpb::Command &cmd);
    Status ApplyRawDelete(const raft_cmdpb::Command &cmd);

    Status ApplyWatchPut(const raft_cmdpb::Command &cmd, uint64_t raftIdx);
    Status ApplyWatchDel(const raft_cmdpb::Command &cmd, uint64_t raftIdx);

    Status ApplyInsert(const raft_cmdpb::Command &cmd);
    Status ApplyDelete(const raft_cmdpb::Command &cmd);

    Status ApplySplit(const raft_cmdpb::Command &cmd, uint64_t index);

    Status ApplyAddPeer(const raft::ConfChange &cc, bool *updated);
    Status ApplyDelPeer(const raft::ConfChange &cc, bool *updated);
    Status ApplyPromotePeer(const raft::ConfChange &cc, bool *updated);

    Status ApplyKVSet(const raft_cmdpb::Command &cmd);
    Status ApplyKVBatchSet(const raft_cmdpb::Command &cmd);
    Status ApplyKVDelete(const raft_cmdpb::Command &cmd);
    Status ApplyKVBatchDelete(const raft_cmdpb::Command &cmd);
    Status ApplyKVRangeDelete(const raft_cmdpb::Command &cmd);

    Status ApplyLock(const raft_cmdpb::Command &cmd);
    Status ApplyLockUpdate(const raft_cmdpb::Command &cmd);
    Status ApplyUnlock(const raft_cmdpb::Command &cmd);
    Status ApplyUnlockForce(const raft_cmdpb::Command &cmd);

    // split func
    void CheckSplit(uint64_t size);
    void AskSplit(std::string &&key, metapb::Range&& meta, bool force = false);
    void ReportSplit(const metapb::Range &new_range);

    int64_t checkMaxCount(int64_t maxCount) {
        if (maxCount <= 0) maxCount = std::numeric_limits<int64_t>::max();
        if (maxCount > max_count_) {
            //FLOG_WARN("%ld exceeded maxCount(%ld)", maxCount, max_count_);
            maxCount = max_count_;
        }
        return maxCount;
    }

    template <class R>
    void SendError(common::ProtoMessage *msg, const kvrpcpb::RequestHeader &req, R *resp,
                   errorpb::Error *err) {
        auto header = resp->mutable_header();

        common::SetResponseHeader(req, header, err);
        context_->SocketSession()->Send(msg, resp);
    }

    template <class R>
    void ReplySubmit(const raft_cmdpb::Command& cmd, R *resp, errorpb::Error *err, int64_t apply_time) {
        auto ctx = submit_queue_.Remove(cmd.cmd_id().seq());
        if (ctx != nullptr) {
            context_->Statistics()->PushTime(monitor::HistogramType::kRaft, apply_time - ctx->CreateTime());
            ctx->CheckExecuteTime(id_, kTimeTakeWarnThresoldUSec);
            ctx->Reply(context_->SocketSession(), resp, err);
        } else {
            RANGE_LOG_WARN("Apply cmd id %" PRIu64 " not found", cmd.cmd_id().seq());
            delete resp;
            delete err;
        }
    }

public:
    // Admin
    void AdminSplit(mspb::AskSplitResponse &resp);

    void AddPeer(const metapb::Peer &peer);
    void DelPeer(const metapb::Peer &peer);

    void ResetStatisSize();
    void Heartbeat();

    Status Destroy();

    // get private member
public:
    bool valid() { return valid_; }
    metapb::Range options() const { return meta_.Get(); }
    bool EpochIsEqual(const metapb::Range &meta) {
        return EpochIsEqual(meta.range_epoch());
    };
    void SetRealSize(uint64_t rsize) { real_size_ = rsize; }
    void GetReplica(metapb::Replica *rep);
    uint64_t GetSplitRangeID() const { return split_range_id_; }
    size_t GetSubmitQueueSize() const { return submit_queue_.Size(); }

    void setLeaderFlag(bool flag) {
        is_leader_ = flag;
    }

private:
    bool VerifyLeader(errorpb::Error *&err);
    bool VerifyReadable(uint64_t read_index, errorpb::Error *&err);
    bool CheckWriteable();
    bool KeyInRange(const std::string &key);
    bool KeyInRange(const std::string &key, errorpb::Error *&err);

    bool EpochIsEqual(const metapb::RangeEpoch &epoch);
    bool EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&);

    bool PushHeartBeatMessage();

    Status SaveMeta(const metapb::Range &meta);

    errorpb::Error *RaftFailError();
    errorpb::Error *NoLeaderError();
    errorpb::Error *NotLeaderError(metapb::Peer &&peer);
    errorpb::Error *KeyNotInRange(const std::string &key);
    errorpb::Error *StaleEpochError(const metapb::RangeEpoch &epoch);
    errorpb::Error *StaleReadIndexError(uint64_t read_index, uint64_t current_index);

private:
    friend class ::sharkstore::test::helper::RangeTestFixture;

    int32_t WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, const int64_t &version, std::string &errMsg, bool prefix = false);
    int32_t loadFromDb(const watchpb::EventType &evtType,
                       const std::string &fromKey,
                       const std::string &endKey,
                       const int64_t &startVersion,
                       watchpb::DsWatchResponse *dsResp);
    int32_t SendNotify( watch::WatcherPtr w, watchpb::DsWatchResponse *ds_resp, bool prefix = false);

private:
    static const int kTimeTakeWarnThresoldUSec = 500000;

    RangeContext* context_ = nullptr;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0;
    // cache range's start key
    // since it will not change unless we have merge operation
    const std::string start_key_;

    MetaKeeper meta_;

    std::atomic<bool> valid_ = { true };

    uint64_t apply_index_ = 0;
    std::atomic<bool> is_leader_ = {false};

    uint64_t real_size_ = 0;
    std::atomic<bool> statis_flag_ = {false};
    std::atomic<uint64_t> statis_size_ = {0};
    uint64_t split_range_id_ = 0;

    watch::CEventBuffer *eventBuffer = nullptr;
    SubmitQueue submit_queue_;

    std::unique_ptr<storage::Store> store_;
    std::shared_ptr<raft::Raft> raft_;

    int64_t max_count_ = 1000;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
