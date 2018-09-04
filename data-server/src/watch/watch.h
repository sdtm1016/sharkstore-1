#ifndef _WATCH_H_
#define _WATCH_H_

namespace sharkstore {
namespace dataserver {
namespace watch {

typedef int64_t WatcherId; // socket session id
typedef int64_t RangeId;
typedef std::string Key;
typedef std::string Prefix;
typedef std::string WatcherKey;
typedef std::string PrefixKey;

enum WatchCode {
    WATCH_OK = 0,
    WATCH_KEY_EXIST,
    WATCH_KEY_NOT_EXIST,
    WATCH_WATCHER_EXIST,
    WATCH_WATCHER_NOT_EXIST,
    WATCH_WATCHER_NOT_NEED
};

enum WatchType {
    WATCH_KEY = 0,
    WATCH_PREFIX
};

}
}
}

#endif
