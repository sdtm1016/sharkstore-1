#include <cassert>

#include "worker_pool.h"
#include "worker.h"
#include "task.h"

namespace sharkstore {
namespace raft {
namespace impl {

void SnapWorkerPool::FreeList::Add(SnapWorker *w) {
    std::unique_lock<std::mutex> lock(mu_);
    workers_.push_back(w);
}

SnapWorker* SnapWorkerPool::FreeList::Get() {
    SnapWorker *w = nullptr;
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!workers_.empty()) {
            w = workers_.front();
            workers_.pop_front();
        }
    }
    return w;
}

size_t SnapWorkerPool::RunningMap::Size() const {
    std::unique_lock<std::mutex> lock(mu_);
    return tasks_.size();
}

void SnapWorkerPool::RunningMap::Add(SnapTaskPtr task) {
    std::unique_lock<std::mutex> lock(mu_);
    tasks_.emplace(task->ID(), task);
}

void SnapWorkerPool::RunningMap::Remove(const SnapTaskPtr& task) {
    std::unique_lock<std::mutex> lock(mu_);
    tasks_.erase(task->ID());
}

void SnapWorkerPool::RunningMap::GetAll(std::vector<SnapTaskPtr> *result) {
    std::unique_lock<std::mutex> lock(mu_);
    for (const auto& p: tasks_) {
        result->push_back(p.second);
    }
}

SnapWorkerPool::SnapWorkerPool(const std::string& name, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        auto w = new SnapWorker(this, name + ":" + std::to_string(i));
        all_workers_.push_back(w);
        free_workers_.Add(w);
    }
}

SnapWorkerPool::~SnapWorkerPool() {
    // cancel running tasks
    std::vector<SnapTaskPtr> tasks;
    running_tasks_.GetAll(&tasks);
    for (auto t : tasks) {
        t->Cancel();
    }

    for (auto w : all_workers_) {
        delete w;
    }
}

bool SnapWorkerPool::Post(const SnapTaskPtr& task) {
    SnapWorker* w = free_workers_.Get();
    if (w != nullptr) {
        w->Post(task);
        return true;
    } else {
        return false;
    }
}

void SnapWorkerPool::addFreeWorker(SnapWorker* w) {
    free_workers_.Add(w);
}

size_t SnapWorkerPool::RunningsCount() const {
    return running_tasks_.Size();
}

void SnapWorkerPool::GetRunningTasks(std::vector<SnapTaskPtr> *tasks) {
    return running_tasks_.GetAll(tasks);
}

void SnapWorkerPool::addRunning(const SnapTaskPtr& task) {
    running_tasks_.Add(task);
}

void SnapWorkerPool::removeRunning(const SnapTaskPtr& task) {
    running_tasks_.Remove(task);
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
