#include "io_context_pool.h"

#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace net {

IOContextPool::IOContextPool(size_t size) : pool_size_(size) {
    for (size_t i = 0; i < size; ++i) {
        auto context = std::make_shared<asio::io_context>();
        auto guard = asio::make_work_guard(*context);
        io_contexts_.push_back(std::move(context));
        work_guards_.push_back(std::move(guard));
    }
}

IOContextPool::~IOContextPool() {
    // stop if running
    Stop();
}

void IOContextPool::Start() {
    int i = 0;
    for (const auto& ctx : io_contexts_) {
        std::thread t(std::bind(&IOContextPool::runLoop, this, ctx, i++));
        threads_.push_back(std::move(t));
    }
}

void IOContextPool::Stop() {
    if (stopped_) return;

    stopped_ = true;
    work_guards_.clear();
    for (auto& ctx : io_contexts_) {
        ctx->stop();
    }
    for (auto& t : threads_) {
        t.join();
    }
}

asio::io_context& IOContextPool::GetIOContext() {
    auto idx = round_robin_counter_++ % io_contexts_.size();
    return *(io_contexts_[idx]);
}

void IOContextPool::runLoop(const std::shared_ptr<asio::io_context>& ctx, int i) {
    FLOG_INFO("[Net] context pool loop-%d start.", i);

    try {
        ctx->run();
    } catch (std::exception& e) {
        FLOG_ERROR("[Net] context pool loop-%d run error: %s.", i, e.what());
    }

    FLOG_INFO("[Net] context pool loop-%d exit.", i);
}

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
