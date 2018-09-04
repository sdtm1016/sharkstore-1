#include <common/socket_session.h>
#include <common/socket_session_impl.h>
//#include "range.h"
#include "watch.h" // old
#include "watch/watcher.h" // new
#include "range/range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

int16_t WatchEncodeAndDecode::EncodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, const watchpb::WatchKeyValue &kv,
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err) {
    int16_t ret(0);
    std::vector<std::string*> keys;
    std::string funcName("");
    std::string ext("");
    keys.clear();
    db_key.clear();

    switch (funcId) {
        case funcpb::kFuncWatchGet:
        case funcpb::kFuncPureGet:
        case funcpb::kFuncWatchPut:
        case funcpb::kFuncWatchDel: {
            if (funcId == funcpb::kFuncWatchGet) {
                funcName.assign("WatchGet");
            } else if (funcId == funcpb::kFuncPureGet) {
                funcName.assign("PureGet");
            } else if (funcId == funcpb::kFuncWatchPut) {
                funcName.assign("WatchPut");
            } else {
                funcName.assign("WatchDel");
            }

            for (auto i = 0; i < kv.key_size(); i++) {
                keys.push_back(new std::string(kv.key(i)));

                FLOG_DEBUG("range[%"
                                   PRIu64
                                   "] EncodeKv:(%s) key%d):%s", meta_.id(), funcName.data(), i,
                           kv.key(i).data());
            }

            watch::Watcher w(meta_.table_id(), keys);


            if (kv.key_size()) {
                w.EncodeKey(&db_key, meta_.table_id(), keys);
            } else {
                ret = -1;
                if (db_key.empty() || kv.key_size() < 1) {
                    FLOG_WARN("range[%"
                                      PRIu64
                                      "] %s error: key empty", meta_.id(), funcName.data());
                    //err = errorpb::KeyNotInRange(db_key);
                    break;
                }
            }
            FLOG_DEBUG("range[%"
                               PRIu64
                               "] %s info: table_id:%"
                               PRId64
                               " key before:%s after:%s",
                       meta_.id(), funcName.data(), meta_.table_id(), keys[0]->data(),
                       EncodeToHexString(db_key).c_str());

            if (!kv.value().empty()) {
                int64_t tmpVersion = kv.version();
                w.EncodeValue(&db_value, tmpVersion, &kv.value(), &ext);

                FLOG_DEBUG("range[%"
                                   PRIu64
                                   "] %s info: value before:%s after:%s",
                           meta_.id(), funcName.data(), kv.value().data(), EncodeToHexString(db_value).c_str());
            }
            break;
        }
        default:
            ret = -1;
            err->set_message("unknown func_id");
            FLOG_WARN("range[%" PRIu64 "] error: unknown func_id:%d", meta_.id(), funcId);
            break;
    }
    return ret;
}


int16_t WatchEncodeAndDecode::DecodeKv(funcpb::FunctionID funcId, const uint64_t &tableId, watchpb::WatchKeyValue *kv,
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err) {
        int16_t ret(0);
        std::vector<std::string*> keys;
        int64_t version(0);
        keys.clear();

        //FLOG_DEBUG("DecodeKv dbKey:%s dbValue:%s", EncodeToHexString(db_key).c_str(), EncodeToHexString(db_value).c_str());
        //FLOG_WARN("range[%" PRIu64 "] DecodeKv dbKey:%s dbValue:%s", meta_.id(), EncodeToHexString(db_key).c_str(), EncodeToHexString(db_value).c_str());
        auto val = std::make_shared<std::string>("");
        auto ext = std::make_shared<std::string>("");

        switch (funcId) {
            case funcpb::kFuncWatchGet:
            case funcpb::kFuncPureGet:
            case funcpb::kFuncWatchPut:
            case funcpb::kFuncWatchDel: {
                std::vector<std::string *> encodeKeys;
                watch::Watcher w(tableId, encodeKeys);

                //decode key to kv
                if (!db_key.empty()) {

                    if (w.DecodeKey(encodeKeys, db_key)) {
                        kv->clear_key();
                        for (auto itKey:encodeKeys) {
                            kv->add_key(*itKey);
                        }
                    } else {
                        FLOG_WARN(" DecodeKey exception, key:%s.", EncodeToHexString(db_key).c_str());
                    }

                    //to do free keys
                    {
                        for (auto itKey:encodeKeys) {
                            free(itKey);
                        }
                    }
                }

                if(!db_value.empty()) {
                    //decode value to kv
                    w.DecodeValue(&version, val.get(), ext.get(), db_value);

//                    FLOG_DEBUG(" version(decode from value)[%"
//                                      PRIu64
//                                      "] key_size:%d  encodevalue:%s", version, kv->key_size(), (*val).c_str());


                    kv->set_value(*val);
                    kv->set_version(version);
                    kv->set_ext(*ext);
                }
                break;
            }
            default:
                ret = -1;
                err->set_message("unknown func_id");
                FLOG_WARN(" error: unknown func_id:%d", funcId);
                break;
        }
        return ret;
}

int16_t  WatchEncodeAndDecode::NextComparableBytes(const char *key, const int32_t &len, std::string &result) {
    if(len > 0) {
        result.resize(len);
    }

    for( auto i = len - 1; i >= 0; i--) {

        uint8_t keyNo = static_cast<uint8_t>(key[i]);

        if( keyNo < 0xff) {
            keyNo++;
            //*result = key;
            result[i] = static_cast<char>(keyNo);

            return 0;
        }
    }

    return -1;
}

} //end namespace range
} //end namespace data-server
} //end namespace sharkstore

