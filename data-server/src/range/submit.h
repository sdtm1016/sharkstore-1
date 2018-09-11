_Pragma("once");

#include <queue>
#include <vector>
#include <unordered_map>
#include <mutex>

#include "proto/gen/raft_cmdpb.pb.h"
#include "common/socket_session.h"

namespace sharkstore {
namespace dataserver {
namespace range {

// 提交raft命令之前创建一个SubmitContext保存上下文到队列中
// 等复制并Apply完成后，再从队列里拿出来给客户端回应

class SubmitContext {
public:
    SubmitContext(const kvrpcpb::RequestHeader &req_header,
            raft_cmdpb::CmdType type, common::ProtoMessage *msg);

    ~SubmitContext();

    SubmitContext(const SubmitContext&) = delete;
    SubmitContext& operator=(const SubmitContext&) = delete;

    common::ProtoMessage* Msg() const { return msg_; }
    void ClearMsg() { msg_ = nullptr; }
    int64_t CreateTime() const { return create_time_; }
    raft_cmdpb::CmdType Type() const { return type_; }

    template <class ResponseT>
    void Reply(common::SocketSession* session, ResponseT* resp, errorpb::Error *err = nullptr) {
        // 设置response头部
        auto header = resp->mutable_header();
        header->set_cluster_id(cluster_id_);
        header->set_trace_id(trace_id_);
        if (err != nullptr) {
            header->set_allocated_error(err);
        }
        // 发送
        session->Send(msg_, resp);
        // Send里面会delete msg_，所以这里置为nullptr，不再重复释放
        msg_ = nullptr;
    }

    void SendTimeout(common::SocketSession* session);

    void CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs);

private:
    // save for set response header lately
    uint64_t cluster_id_ = 0;
    uint64_t trace_id_ = 0;

    int64_t create_time_ = 0;
    raft_cmdpb::CmdType type_;
    common::ProtoMessage *msg_ = nullptr;
};

class SubmitQueue {
public:
    SubmitQueue() = default;
    ~SubmitQueue() = default;

    SubmitQueue(const SubmitQueue&) = delete;
    SubmitQueue& operator=(const SubmitQueue&) = delete;

    // 只获取一个递增的ID, for split command
    uint64_t GetSeq();

    uint64_t Add(const kvrpcpb::RequestHeader& req_header,
                 raft_cmdpb::CmdType type, common::ProtoMessage *msg);

    std::unique_ptr<SubmitContext> Remove(uint64_t seq_id);

    std::vector<uint64_t> GetExpired(size_t max_count = 10000);

    size_t Size() const;

private:
    using ExpirePair = std::pair<time_t, uint64_t>;
    using ExpireQueue = std::priority_queue<ExpirePair, std::vector<ExpirePair>, std::greater<ExpirePair>>;
    using SubmitContextPtr = std::unique_ptr<SubmitContext>;
    using ContextMap = std::unordered_map<uint64_t, SubmitContextPtr>;

    uint64_t seq_ = 0;
    ContextMap ctx_map_;
    ExpireQueue expire_que_;
    mutable std::mutex mu_;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
