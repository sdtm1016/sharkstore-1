#include "worker.h"

#include <cassert>
#include "base/util.h"

#include "task.h"
#include "worker_pool.h"

namespace sharkstore {
namespace raft {
namespace impl {

SnapWorker::SnapWorker(SnapWorkerPool* pool, const std::string& name) : pool_(pool) {
    thr_ = std::thread([this] { runTask(); });
    AnnotateThread(thr_.native_handle(), name.c_str());
}

SnapWorker::~SnapWorker() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
        if (task_ != nullptr) {
            task_->Cancel();
        }
    }
    cv_.notify_one();
    thr_.join();
}

void SnapWorker::Post(const std::shared_ptr<SnapTask>& task) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        assert(task_ == nullptr);
        task_ = task;
    }
    cv_.notify_one();
}

void SnapWorker::runTask() {
    while (true) {
        std::shared_ptr<SnapTask> task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            while (task_ == nullptr && running_) {
                cv_.wait(lock);
            }
            if (!running_) return;
            task = std::move(task_);
            task_ = nullptr;
        }

        pool_->addRunning(task);
        task->Run();
        pool_->removeRunning(task);

        // 归还worker
        pool_->addFreeWorker(this);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
