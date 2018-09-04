_Pragma("once");

#include <string>
#include <memory>
#include <condition_variable>
#include <thread>

namespace sharkstore {
namespace raft {
namespace impl {

class SnapTask;
class SnapWorkerPool;

class SnapWorker final {
public:
    explicit SnapWorker(SnapWorkerPool* pool, const std::string& thread_name);
    ~SnapWorker();

    SnapWorker(const SnapWorker&) = delete;
    SnapWorker& operator=(const SnapWorker&) = delete;

    void Post(const std::shared_ptr<SnapTask>& task);

private:
    void runTask();

private:
    SnapWorkerPool* pool_ = nullptr;

    bool running_ = true;

    std::shared_ptr<SnapTask> task_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread thr_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
