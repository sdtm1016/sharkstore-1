#ifndef __RANGE_MANAGER_H__
#define __RANGE_MANAGER_H__

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"

#include "base/shared_mutex.h"
#include "base/status.h"
#include "master/task_handler.h"
#include "range/range.h"
#include "storage/meta_store.h"

#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RangeServer final : public master::TaskHandler {
public:
    RangeServer() = default;
    ~RangeServer() = default;

    RangeServer(const RangeServer &) = delete;
    RangeServer &operator=(const RangeServer &) = delete;
    RangeServer &operator=(const RangeServer &) volatile = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();
    void Clear();

    void DealTask(common::ProtoMessage *msg);
    void StatisPush(uint64_t range_id);

    storage::MetaStore *meta_store() { return meta_store_; }
    metapb::Range *GetRangeMeta(uint64_t range_id);

    std::shared_ptr<range::Range> find(uint64_t range_id);

    void OnNodeHeartbeatResp(const mspb::NodeHeartbeatResponse &) override;
    void OnRangeHeartbeatResp(const mspb::RangeHeartbeatResponse &) override;
    void OnAskSplitResp(const mspb::AskSplitResponse &) override;
    void CollectNodeHeartbeat(mspb::NodeHeartbeatRequest *req) override;

private:
    int OpenDB();
    void CloseDB();
    int Recover(std::vector<std::string> &metas);

    void RawGet(common::ProtoMessage *msg);
    void RawPut(common::ProtoMessage *msg);
    void RawDelete(common::ProtoMessage *msg);
    void Insert(common::ProtoMessage *msg);
    void Select(common::ProtoMessage *msg);
    void Delete(common::ProtoMessage *msg);

    void Lock(common::ProtoMessage *msg);
    void LockUpdate(common::ProtoMessage *msg);
    void Unlock(common::ProtoMessage *msg);
    void UnlockForce(common::ProtoMessage *msg);
    void LockScan(common::ProtoMessage *msg);

    void KVSet(common::ProtoMessage *msg);
    void KVGet(common::ProtoMessage *msg);
    void KVBatchSet(common::ProtoMessage *msg);
    void KVBatchGet(common::ProtoMessage *msg);
    void KVDelete(common::ProtoMessage *msg);
    void KVBatchDelete(common::ProtoMessage *msg);
    void KVRangeDelete(common::ProtoMessage *msg);
    void KVScan(common::ProtoMessage *msg);

    void TimeOut(const kvrpcpb::RequestHeader &req,
                 kvrpcpb::ResponseHeader *resp);
    void RangeNotFound(const kvrpcpb::RequestHeader &req,
                       kvrpcpb::ResponseHeader *resp);

    template <class RequestT, class ResponseT>
    std::shared_ptr<range::Range> CheckAndDecodeRequest(
        const char *ReqName, RequestT &request, ResponseT *&respone,
        common::ProtoMessage *msg);

public:
    Status ApplySplit(uint64_t old_range_id,
                      const raft_cmdpb::SplitRequest &req);
    void LeaderQueuePush(uint64_t leader, time_t expire);

    void ResetStats();

private:  // admin
    void CreateRange(common::ProtoMessage *msg);
    void DeleteRange(common::ProtoMessage *msg);
    void UpdateRange(common::ProtoMessage *msg);
    void OfflineRange(common::ProtoMessage *msg);
    void ReplaceRange(common::ProtoMessage *msg);

    void TransferLeader(common::ProtoMessage *msg);
    void GetPeerInfo(common::ProtoMessage *msg);
    void SetLogLevel(common::ProtoMessage *msg);

    Status CreateRange(const metapb::Range &range, uint64_t leader = 0);
    int DeleteRange(uint64_t range_id);
    int CloseRange(uint64_t range_id);
    int OfflineRange(uint64_t range_id);

    void Heartbeat();

private:
    shared_mutex rw_lock_;
    std::unordered_map<int64_t, std::shared_ptr<range::Range>> ranges_;

    std::mutex statis_mutex_;
    std::condition_variable statis_cond_;
    std::queue<uint64_t> statis_queue_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cond_;

    typedef std::pair<time_t, uint64_t> tr;
    std::priority_queue<tr, std::vector<tr>, std::greater<tr>>
        range_heartbeat_queue_;

    std::vector<std::thread> worker_;
    std::thread range_heartbeat_;

    rocksdb::DB *db_ = nullptr;
    storage::MetaStore *meta_store_ = nullptr;

    ContextServer *context_ = nullptr;
    range_status_t *range_status_ = nullptr;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore

#endif  //__RANGE_MANAGER_H__
