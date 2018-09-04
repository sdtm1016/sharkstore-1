_Pragma("once");

#include <memory>
#include <chrono>
#include <thread>
#include <queue>
#include <condition_variable>

namespace sharkstore {

class Timer {
public:
    virtual ~Timer() = default;
    virtual void OnTimeout() = 0;
};

class TimerQueue {
public:
    TimerQueue();
    virtual ~TimerQueue();

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    void Push(const std::shared_ptr<Timer>& timer, int timeout_msec);
    void Stop();

private:
    void run();
    // false means exit
    bool waitOneExpire(std::weak_ptr<Timer> *timer);

private:
    struct Item {
        std::weak_ptr<Timer> timer;
        std::chrono::steady_clock::time_point expire_at;

        bool Expired() const {
            return expire_at <= std::chrono::steady_clock::now();
        }

        bool operator > (const Item& other) const {
            return expire_at > other.expire_at;
        }
    };

    using QueueType = std::priority_queue<Item, std::vector<Item>, std::greater<Item>>;

private:
    bool stopped_ = false;
    QueueType queue_;
    std::mutex mu_;
    std::condition_variable cond_;
    std::thread thr_;
};


} /* namespace sharkstore */


