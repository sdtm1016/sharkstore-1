_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <functional>
#include <unordered_map>
#include "raft_types.h"

namespace sharkstore {
namespace raft {
namespace impl {

class RaftImpl;
class RaftServerImpl;

static const int kMaxBatchSize = 64;

struct Work {
    uint64_t owner = 0;
    std::atomic<bool>* stopped = nullptr;

    std::function<void()> f0;

    std::function<void(MessagePtr&)> f1;
    MessagePtr msg = nullptr;

    void Do();
};

class WorkThread {
public:
    WorkThread(RaftServerImpl* server, size_t queue_capcity,
               const std::string& name = "raft-worker");
    ~WorkThread();

    WorkThread(const WorkThread&) = delete;
    WorkThread& operator=(const WorkThread&) = delete;

    bool submit(uint64_t owner, std::atomic<bool>* stopped,
                const std::function<void(MessagePtr&)>& f1, std::string& cmd);

    bool tryPost(const Work& w);
    void post(const Work& w);
    void waitPost(const Work& w);
    void shutdown();
    int size() const;

private:
    bool pull(Work* w);
    void run();

private:
    RaftServerImpl* server_ = nullptr;
    const size_t capacity_ = 0;

    std::unique_ptr<std::thread> thr_;
    bool running_ = false;
    std::queue<Work> queue_;
    // 记录每个range最近一条LOCAL_MSG_PROP消息，便于batch合并
    std::unordered_map<uint64_t, MessagePtr> batch_pos_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
