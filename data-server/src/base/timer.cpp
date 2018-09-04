#include "timer.h"

namespace sharkstore {

TimerQueue::TimerQueue() {
    thr_ = std::thread([this] { run(); });
}

TimerQueue::~TimerQueue() {
    Stop();
}

void TimerQueue::Push(const std::shared_ptr<Timer>& timer, int timeout_msec) {
    Item item;
    item.timer = timer;
    item.expire_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_msec);

    std::lock_guard<std::mutex> lock(mu_);
    queue_.push(item);
    cond_.notify_one();
}

void TimerQueue::Stop() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) return;
        stopped_ = true;
    }
    cond_.notify_one();
    thr_.join();
}

bool TimerQueue::waitOneExpire(std::weak_ptr<Timer> *timer) {
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        if (!stopped_ && queue_.empty()) {
            cond_.wait(lock);
        }
        if (stopped_) return false;
        if (queue_.top().Expired()) {
            *timer = queue_.top().timer;
            queue_.pop();
            return true;
        } else {
            cond_.wait_until(lock, queue_.top().expire_at);
        }
    }
}

void TimerQueue::run() {
    while (true) {
        std::weak_ptr<Timer> wp_timer;
        auto ret = waitOneExpire(&wp_timer);
        if (!ret) { // over
            return;
        }
        try {
            auto sp_timer = wp_timer.lock();
            if (sp_timer) {
                sp_timer->OnTimeout();
            }
        } catch (std::exception& e) {
            fprintf(stderr, "OnTimeout exception: %s", e.what());
        }
    }
}


} /* namespace sharkstore */

