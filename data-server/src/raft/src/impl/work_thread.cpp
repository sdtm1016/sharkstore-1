#include "work_thread.h"

#include <assert.h>
#include <thread>
#include "base/util.h"
#include "logger.h"
#include "raft_exception.h"
#include "server_impl.h"

namespace sharkstore {
namespace raft {
namespace impl {

void Work::Do() {
    if (!(*stopped)) {
        if (msg == nullptr) {
            f0();
        } else {
            f1(msg);
        }
    }
}

WorkThread::WorkThread(RaftServerImpl* server, size_t queue_capcity,
                       const std::string& name)
    : server_(server), capacity_(queue_capcity), running_(true) {
    assert(server_ != nullptr);
    assert(capacity_ > 0);

    thr_.reset(new std::thread(std::bind(&WorkThread::run, this)));
    // 设置线程名称
    AnnotateThread(thr_->native_handle(), name.c_str());
}

WorkThread::~WorkThread() { shutdown(); }

bool WorkThread::submit(uint64_t owner, std::atomic<bool>* stopped,
                        const std::function<void(MessagePtr&)>& f1,
                        std::string& cmd) {
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::LOCAL_MSG_PROP);

    bool notify = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;

        auto it = batch_pos_.find(owner);
        if (it != batch_pos_.end() &&
            it->second->entries_size() < kMaxBatchSize) {
            // 可以合并
            auto entry = it->second->add_entries();
            entry->set_type(pb::ENTRY_NORMAL);
            entry->mutable_data()->swap(cmd);
        } else if (queue_.size() >= capacity_) {
            return false;
        } else {
            // 不能合并，new一个
            auto entry = msg->add_entries();
            entry->set_type(pb::ENTRY_NORMAL);
            entry->mutable_data()->swap(cmd);
            Work w;
            w.owner = owner;
            w.stopped = stopped;
            w.f1 = f1;
            w.msg = msg;
            queue_.push(w);
            batch_pos_[owner] = msg;
            notify = true;
        }
    }
    if (notify) {
        cv_.notify_one();
    }
    return true;
}

bool WorkThread::tryPost(const Work& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;
        if (queue_.size() >= capacity_) {
            return false;
        } else {
            queue_.push(w);
        }
    }
    cv_.notify_one();
    return true;
}

void WorkThread::post(const Work& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return;
        queue_.push(w);
    }
    cv_.notify_one();
}

void WorkThread::waitPost(const Work& w) {
    std::unique_lock<std::mutex> lock(mu_);
    while (queue_.size() >= capacity_ && running_) {
        cv_.wait(lock);
    }

    if (running_) {
        queue_.push(w);
        lock.unlock();
        cv_.notify_one();
    }
}

void WorkThread::shutdown() {
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_one();
    thr_->join();
}

bool WorkThread::pull(Work* w) {
    std::unique_lock<std::mutex> lock(mu_);

    while (queue_.empty() && running_) {
        cv_.wait(lock);
    }
    if (!running_) return false;
    *w = queue_.front();
    queue_.pop();

    if (w->msg != nullptr && w->msg->type() == pb::LOCAL_MSG_PROP) {
        assert(w->owner != 0);
        auto it = batch_pos_.find(w->owner);
        if (it != batch_pos_.end() && it->second == w->msg) {
            assert(it->second->type() == pb::LOCAL_MSG_PROP);
            batch_pos_.erase(it);
        }
    }

    lock.unlock();
    cv_.notify_one();  // 通知队列已经不再满了
    return true;
}

void WorkThread::run() {
    while (true) {
        Work work;
        if (pull(&work)) {
            try {
                work.Do();
            } catch (RaftException& e) {
                assert(work.owner > 0);
                LOG_ERROR("raft[%llu] throw an exception: %s. removed.",
                          work.owner, e.what());
                server_->RemoveRaft(work.owner, true);
            }
        } else {
            // shutdown
            return;
        }
    }
}

int WorkThread::size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
