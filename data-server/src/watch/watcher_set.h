_Pragma("once");

#include <unordered_map>

#include "watcher.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

class WatcherList {
public:
    uint64_t Add(Watcher watcher);
    Watcher Delete(uint64_t id);
    void Swap(std::vector<Watcher>* watchers);

private:
    uint64_t last_id_ = 0;
    std::unordered_map<uint64_t, Watcher> watchers_;
};

class WatcherSet {
public:
    virtual ~WatcherSet() = default;

    // 尝试添加Watcher到Watcher列表，
    // 1) 如果WatcherSet里已经有大于version的修改则不添加, 并通过event返回较新的修改
    // 2）否则添加到WatcherSet中，并返回watcher_id
    virtual Status AddWatcher(Watcher watcher, int64_t version,
            std::vector<watchpb::Event>* events, uint64_t *watcher_id) = 0;

    // 新事件发送
    virtual void AddEvent(const watchpb::Event& event) = 0;

    // 删除id对应的watcher，并返回该watcher
    Watcher Expire(uint64_t id) { return watchers_.Delete(id); }
    void SwapAll(std::vector<Watcher> *watchers) { watchers_.Swap(watchers); }

protected:
    WatcherList watchers_;
};

class KeyWatcherSet : public WatcherSet {
public:
    KeyWatcherSet() = default;

    Status AddWatcher(Watcher watcher, int64_t version,
                 std::vector<watchpb::Event>* events,
                 uint64_t *watcher_id) override;

    void AddEvent(const watchpb::Event& event) override;

private:
    std::unique_ptr<watchpb::Event> latest_;
};

class PrefixWatcherSet : public WatcherSet {
public:
    explicit PrefixWatcherSet(size_t max_history = 1000) :
        capacity_(max_history) {}

    Status AddWatcher(Watcher watcher, int64_t version,
                  std::vector<watchpb::Event>* events,
                  uint64_t *watcher_id) override;

    void AddEvent(const watchpb::Event& event) override;

private:
    void getEvents(int64_t version, std::vector<watchpb::Event> *events);

private:
    const size_t capacity_ = 0;
    std::deque<watchpb::Event> history_;
};

} // namespace watch
}
}
