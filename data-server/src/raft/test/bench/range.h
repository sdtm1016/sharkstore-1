_Pragma("once");

#include <future>
#include <unordered_map>

#include "raft/raft.h"
#include "raft/server.h"

namespace sharkstore {
namespace raft {
namespace bench {

class NodeAddress;

class Range : public raft::StateMachine,
              public std::enable_shared_from_this<Range> {
public:
    Range(uint64_t id, uint64_t node_id, RaftServer* rs,
          const std::shared_ptr<NodeAddress>& addr_mgr);
    ~Range();

    void Start();

    void WaitLeader();
    bool IsLeader() const { return leader_ == node_id_; }
    void SyncRequest();
    std::shared_future<bool> AsyncRequest();

public:
    Status Apply(const std::string& cmd, uint64_t index) override;

    void OnLeaderChange(uint64_t leader, uint64_t term) { leader_ = leader; }
    void OnReplicateError(const std::string& cmd, const Status& status) {}
    Status ApplyMemberChange(const ConfChange&, uint64_t) {
        return Status::OK();
    }
    std::shared_ptr<raft::Snapshot> GetSnapshot() { return nullptr; }
    Status ApplySnapshotStart(const std::string&) {
        return Status(Status::kNotSupported);
    }
    Status ApplySnapshotData(const std::vector<std::string>&) {
        return Status(Status::kNotSupported);
    }
    Status ApplySnapshotFinish(uint64_t) {
        return Status(Status::kNotSupported);
    }

private:
    class RequestQueue {
    public:
        uint64_t add(std::shared_future<bool>* f);
        void set(uint64_t seq, bool value);
        void remove(uint64_t seq);

    private:
        std::unordered_map<uint64_t, std::promise<bool>> que_;
        std::mutex mu_;
        uint64_t seq_ = 0;
    };

private:
    const uint64_t id_ = 0;
    const uint64_t node_id_ = 0;
    RaftServer* raft_server_ = nullptr;
    std::shared_ptr<NodeAddress> addr_mgr_;

    std::shared_ptr<Raft> raft_;
    uint64_t leader_ = 0;

    RequestQueue request_queue_;
};

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */
