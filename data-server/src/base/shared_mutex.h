_Pragma("once");

#include <errno.h>
#include <pthread.h>
#include <utility>

namespace sharkstore {

class __shared_mutex_pthread {
    pthread_rwlock_t m_rwlock_ = PTHREAD_RWLOCK_INITIALIZER;

public:
    __shared_mutex_pthread() = default;
    ~__shared_mutex_pthread() = default;

    __shared_mutex_pthread(const __shared_mutex_pthread&) = delete;
    __shared_mutex_pthread& operator=(const __shared_mutex_pthread&) = delete;

    void lock() { pthread_rwlock_wrlock(&m_rwlock_); }

    void unlock() { pthread_rwlock_unlock(&m_rwlock_); }

    void lock_shared() {
        int ret;
        do {
            ret = pthread_rwlock_rdlock(&m_rwlock_);
        } while (ret == EAGAIN);
    }

    void unlock_shared() { unlock(); }
};

class shared_mutex {
public:
    shared_mutex() = default;
    ~shared_mutex() = default;

    shared_mutex(const shared_mutex&) = delete;
    shared_mutex& operator=(const shared_mutex&) = delete;

    void lock() { m_impl_.lock(); }
    void unlock() { m_impl_.unlock(); }

    // Shared ownership
    void lock_shared() { m_impl_.lock_shared(); }
    void unlock_shared() { m_impl_.unlock_shared(); }

private:
    __shared_mutex_pthread m_impl_;
};

template <typename Mutex>
class shared_lock {
public:
    typedef Mutex mutex_type;

    shared_lock() noexcept : m_pm_(nullptr) {}

    explicit shared_lock(mutex_type& m) : m_pm_(std::addressof(m)) {
        m.lock_shared();
    }

    ~shared_lock() { m_pm_->unlock_shared(); }

    shared_lock(shared_lock const&) = delete;
    shared_lock& operator=(shared_lock const&) = delete;

    void lock() { m_pm_->lock_shared(); }

    void unlock() { m_pm_->unlock_shared(); }

private:
    mutex_type* m_pm_;
};

}  // namespace sharkstore
