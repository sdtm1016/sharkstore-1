#ifndef _WATCHER_H_
#define _WATCHER_H_

#include <mutex>
#include <unordered_map>
#include <atomic>

#include "watch.h"
#include "common/socket_session.h"
#include "storage/store.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

using WatchKey = google::protobuf::RepeatedPtrField<std::string>;
using Watcher = common::ProtoMessage*;

} // namespace watch
}
}

#endif
