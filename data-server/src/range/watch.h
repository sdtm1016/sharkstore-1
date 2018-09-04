_Pragma("once");
#include <string>
#include <proto/gen/watchpb.pb.h>
#include <proto/gen/funcpb.pb.h>
#include <proto/gen/errorpb.pb.h>
#include <common/socket_session.h>
#include <common/ds_encoding.h>
#include <common/ds_config.h>
#include <watch/watcher.h>

#include <frame/sf_logger.h>
#include <mutex>
#include <memory>
#include <queue>
#include <vector>
#include <functional>
#include <condition_variable>
#include <thread>
#include <unordered_map>

#define MAX_WATCHER_SIZE 100000

namespace sharkstore {
namespace dataserver {
namespace range {

class WatchEncodeAndDecode {
public:
    WatchEncodeAndDecode() {};

    ~WatchEncodeAndDecode() {};

public:
    static int16_t EncodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, const watchpb::WatchKeyValue &kv,
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err);

    static int16_t DecodeKv(funcpb::FunctionID funcId, const uint64_t &tableId, watchpb::WatchKeyValue *kv,
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err);
    
    static int16_t  NextComparableBytes(const char *key, const int32_t &len, std::string &result);
};

class WatchUtil {
public:

    static bool GetHashKey(watch::WatcherPtr pWatcher, bool prefix, const int64_t &tableId, std::string *encodeKey) {

        auto hashKeys = pWatcher->GetKeys(prefix);
        watch::Watcher::EncodeKey(encodeKey, tableId, hashKeys);

        return true;
    }


};


}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
