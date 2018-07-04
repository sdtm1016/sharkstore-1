_Pragma("once");

#include <atomic>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "base/shared_mutex.h"

#include "raft/server.h"
#include "raft_types.h"

namespace sharkstore {
namespace raft {
namespace impl {

class RaftImpl;
class WorkThread;
class SnapshotManager;

namespace transport {
class Transport;
}

class RaftServerImpl : public RaftServer {
public:
    explicit RaftServerImpl(const RaftServerOptions& ops);
    ~RaftServerImpl();

    RaftServerImpl(const RaftServerImpl&) = delete;
    RaftServerImpl& operator=(const RaftServerImpl&) = delete;

    const RaftServerOptions& Options() const { return ops_; }

    Status Start() override;
    Status Stop() override;

    Status CreateRaft(const RaftOptions&, std::shared_ptr<Raft>* raft) override;
    Status RemoveRaft(uint64_t id, bool backup) override;
    std::shared_ptr<Raft> FindRaft(uint64_t id) const override;

    void GetStatus(ServerStatus* status) const override;

private:
    using RaftMapType = std::unordered_map<uint64_t, std::shared_ptr<RaftImpl>>;

    std::shared_ptr<RaftImpl> findRaft(uint64_t id) const;

    void sendHeartbeat(const RaftMapType& rafts);
    void onMessage(MessagePtr& msg);
    void onHeartbeatReq(MessagePtr& msg);
    void onHeartbeatResp(MessagePtr& msg);

    void stepTick(const RaftMapType& rafts);
    void printMetrics();
    void tickRoutine();

private:
    const RaftServerOptions ops_;
    std::atomic<bool> running_ = {false};

    RaftMapType all_rafts_;
    std::unordered_set<uint64_t> creating_rafts_;  // 正在被创建的
    uint64_t create_count_ = 0;
    mutable sharkstore::shared_mutex rafts_mu_;

    std::unique_ptr<transport::Transport> transport_;
    std::unique_ptr<SnapshotManager> snapshot_manager_;

    std::vector<WorkThread*> consensus_threads_;
    std::vector<WorkThread*> apply_threads_;

    MessagePtr tick_msg_;
    // TODO: more tick threads or put ticks into consensus_threads
    std::unique_ptr<std::thread> tick_thr_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
