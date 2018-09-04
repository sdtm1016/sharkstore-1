#ifndef _WATCHER_SET_H_
#define _WATCHER_SET_H_

#include <unordered_map>
#include <vector>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "watch.h"
#include "watcher.h"
#include "storage/store.h"

namespace sharkstore {
namespace dataserver {
namespace watch {



typedef std::unordered_map<WatcherId, WatcherPtr> KeyWatcherMap;
typedef struct WatcherValue_ {
    int64_t key_version_{0};
    KeyWatcherMap mapKeyWatcher;
}WatcherValue;
//typedef std::unordered_map<Key, KeyWatcherMap*> WatcherMap;
typedef std::unordered_map<WatcherKey, WatcherValue*> WatcherMap;

typedef std::unordered_map<WatcherKey, int64_t > WatcherKeyMap;
typedef std::unordered_map<WatcherId, WatcherKeyMap*> KeyMap;

template <typename T>
struct PriorityQueue: public std::priority_queue<T, std::vector<T>, Greater<T>> {
    const std::vector<T>& GetQueue() {
        return this->c;
    }
};

class WatcherSet {
public:
    WatcherSet();
    WatcherSet(const WatcherSet&) = delete;
    WatcherSet& operator=(const WatcherSet&) = delete;
    ~WatcherSet();

    WatchCode AddKeyWatcher(const WatcherKey&, WatcherPtr&, storage::Store *);
    WatchCode DelKeyWatcher(const WatcherKey&, WatcherId);
    WatchCode GetKeyWatchers(const watchpb::EventType &evtType, std::vector<WatcherPtr>& , const WatcherKey&, const int64_t &version);
    WatchCode AddPrefixWatcher(const PrefixKey&, WatcherPtr&, storage::Store *);
    WatchCode DelPrefixWatcher(const PrefixKey&, WatcherId);
    WatchCode GetPrefixWatchers(const watchpb::EventType &evtType, std::vector<WatcherPtr>& , const PrefixKey&, const int64_t &version);
    bool ChgGlobalVersion(const uint64_t &ver) noexcept {
        if(ver <= global_version_)
            return false;
        else
            global_version_ = ver;

        return true;
    };

    uint64_t getVersion() const {
        return global_version_;
    };

    void WatchSetLock(uint16_t flag) {
        if(flag) {
            watcher_map_mutex_.lock();
        } else {
            watcher_map_mutex_.unlock();
        }
    };

    std::pair<int32_t, bool> loadFromDb(storage::Store *store, const watchpb::EventType &evtType, const std::string &fromKey,
                       const std::string &endKey, const int64_t &startVersion, const uint64_t &tableId,
                       watchpb::DsWatchResponse *dsResp);


private:
    WatcherMap              key_watcher_map_;
    KeyMap                  key_map_;
    WatcherMap              prefix_watcher_map_;
    KeyMap                  prefix_map_;
    PriorityQueue<WatcherPtr>   watcher_queue_;
    std::mutex              watcher_map_mutex_;
    std::mutex              watcher_queue_mutex_;
    std::atomic<WatcherId>  watcher_id_ = {0};


    std::thread                     watcher_timer_;
    volatile bool                   watcher_timer_continue_flag_ = true;
    std::condition_variable         watcher_expire_cond_;
    uint64_t                global_version_{0};
private:
    WatchCode AddWatcher(const WatcherKey&, WatcherPtr&, WatcherMap&, KeyMap&, storage::Store *, bool prefixFlag = false);
    WatchCode DelWatcher(const WatcherKey&, WatcherId, WatcherMap&, KeyMap&);
    WatchCode GetWatchers(const watchpb::EventType &evtType, std::vector<WatcherPtr>& vec, const WatcherKey&, WatcherMap&, WatcherValue *watcherVal, bool prefixFlag = false);

public:
    WatcherId GenWatcherId() {
        watcher_id_.fetch_add(1, std::memory_order_relaxed);
        return watcher_id_;
    }
};


} // namespace watch
}
}

#endif
