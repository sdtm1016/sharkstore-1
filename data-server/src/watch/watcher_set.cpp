
#include "watcher_set.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

uint64_t WatcherList::Add(Watcher watcher) {
    watchers_.emplace(++last_id_, watcher);
}

Watcher WatcherList::Delete(uint64_t id) {
    auto it = watchers_.find(id);
    if (it != watchers_.end()) {
        watchers_.erase(it);
        return it->second;
    }
    return nullptr;
}

void WatcherList::Swap(std::vector<Watcher> *watchers) {
    for (const auto& kv : watchers_) {
        watchers->push_back(kv.second);
    }
    watchers_.clear();
}

Status KeyWatcherSet::AddWatcher(Watcher watcher, int64_t version,
                     std::vector<watchpb::Event>* events, uint64_t *watcher_id) {
    if (latest_) {
        auto current = latest_->kv().version();
        if (current > version) {
            events->push_back(*latest_);
            return Status(Status::kExisted);
        } else if (current < version) {
            return Status(Status::kInvalidArgument, std::to_string(version), std::to_string(current));
        }
    }

    *watcher_id = watchers_.Add(watcher);
    return Status::OK();
}

void KeyWatcherSet::AddEvent(const watchpb::Event& event) {
    auto current = latest_ ? latest_->kv().version() : 0;
    assert(event.kv().version() > current);
    (void)current;
    latest_.reset(new watchpb::Event(event));
}



void PrefixWatcherSet::getEvents(int64_t version,
               std::vector<watchpb::Event> *events) {
    auto it = std::lower_bound(history_.cbegin(), history_.cend(), version + 1,
                               [](const watchpb::Event& e, int64_t version) {
                                   return e.kv().version() < version;
                               });
    for (; it != history_.end(); ++it) {
        events->push_back(*it);
    }
}

Status PrefixWatcherSet::AddWatcher(Watcher watcher, int64_t version,
                     std::vector<watchpb::Event>* events, uint64_t *watcher_id) {
    if (!history_.empty()) {
        auto min_version = history_.front().kv().version();
        auto max_version = history_.back().kv().version();
        assert(min_version >= max_version);
        if (version < min_version) {
            return Status(Status::kOutOfBound); // 需要全量
        } else if (version > max_version) {
            return Status(Status::kInvalidArgument, std::to_string(version),
                    std::to_string(max_version));
        } else if (version < max_version) {
            getEvents(version, events);
            return Status(Status::kExisted);
        }
    }
    *watcher_id = watchers_.Add(watcher);
    return Status::OK();
}

void PrefixWatcherSet::AddEvent(const watchpb::Event& event) {
    auto max = history_.empty() ? 0 : history_.back().kv().version();
    assert(event.kv().version() > max);
    (void)max;

    while (history_.size() >= capacity_) {
        history_.pop_front();
    }
    history_.push_back(event);
}

} // namespace watch
}
}
