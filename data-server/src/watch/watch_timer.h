_Pragma("once");

#include <memory>


namespace sharkstore {
namespace dataserver {
namespace watch {

class WatcherSet;

class WatchTimer {
};

class TimerQueue {
public:
    struct Item {
        std::weak_ptr<WatcherSet> s;
        uint64_t id;

    };

private:
};


} // namespace watch
} // namespace dataserver
} // namespace sharkstore
