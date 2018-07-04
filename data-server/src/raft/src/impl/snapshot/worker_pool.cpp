#include <cassert>

#include "worker_pool.h"
#include "worker.h"

namespace sharkstore {
namespace raft {
namespace impl {

SnapWorkerPool::SnapWorkerPool(const std::string& name, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        auto w = new SnapWorker(this, name);
        workers_.push_back(w);
        free_list_.push_back(w);
    }
}

SnapWorkerPool::~SnapWorkerPool() {
    for (auto w : workers_) {
        delete w;
    }
}

bool SnapWorkerPool::Post(const std::shared_ptr<SnapTask>& task) {
    SnapWorker* w = nullptr;

    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!free_list_.empty()) {
            w = free_list_.front();
            free_list_.pop_front();
        }
    }

    if (w != nullptr) {
        w->post(task);
        return true;
    } else {
        return false;
    }
}

size_t SnapWorkerPool::Runnings() const {
    std::lock_guard<std::mutex> lock(mu_);
    assert(workers_.size() >= free_list_.size());
    return workers_.size() - free_list_.size();
}

void SnapWorkerPool::addToFreeList(SnapWorker* w) {
    std::lock_guard<std::mutex> lock(mu_);

    free_list_.push_back(w);
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
