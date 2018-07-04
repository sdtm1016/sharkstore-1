_Pragma("once");

#include <atomic>
#include <thread>
#include <vector>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

namespace sharkstore {
namespace dataserver {
namespace net {

class IOContextPool final {
public:
    explicit IOContextPool(size_t size);
    ~IOContextPool();

    IOContextPool(const IOContextPool&) = delete;
    IOContextPool& operator=(const IOContextPool&) = delete;

    void Start();
    void Stop();

    size_t Size() const { return pool_size_; }

    // must check Size()>0 before
    asio::io_context& GetIOContext();

private:
    void runLoop(const std::shared_ptr<asio::io_context>& ctx, int i);

private:
    using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;

    const size_t pool_size_ = 0;

    std::vector<std::shared_ptr<asio::io_context>> io_contexts_;
    std::vector<WorkGuard> work_guards_;
    bool stopped_ = false;

    std::atomic<uint64_t> round_robin_counter_ = {0};

    std::vector<std::thread> threads_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
