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

class Watcher {
public:
    Watcher() = delete;
    Watcher(uint64_t, const std::vector<WatcherKey*>&, const uint64_t &, const int64_t &, common::ProtoMessage*);
    Watcher(WatchType, uint64_t, const std::vector<WatcherKey*>&, const uint64_t &, const int64_t &, common::ProtoMessage*);
    Watcher(uint64_t, const std::vector<WatcherKey*>&);
    virtual ~Watcher();
    bool operator>(const Watcher* other) const;
    void setBufferFlag(const int64_t &flag){
        buffer_flag_ = flag;
    }
    int64_t getBufferFlag() const {
        return buffer_flag_;
    }

private:
    uint64_t                    table_id_ = 0;
    std::vector<std::string*>   keys_;
    std::vector<std::string*>   keys_hash_;
    int64_t                    key_version_ = 0;
    common::ProtoMessage*       message_ = nullptr;
    WatchType                   type_ = WATCH_KEY;
    WatcherId                   watcher_id_ = 0;
    int64_t                     session_id_ = 0;
    int64_t                     msg_id_ = 0;
    int64_t                     expire_time_ = 0;
    //prefix mode:
    // 0 key has no changing, need to add watcher
    // -1 key version is lower than buffer or buffer is empty, need to get all from db
    int64_t                     buffer_flag_ = 0;

    std::mutex          send_lock_;
    volatile bool       sent_response_flag = false;

public:
    uint64_t GetTableId() { return table_id_; }
    const std::vector<std::string*>& GetKeys(bool hashFlag = true) {
        if(hashFlag) return keys_hash_;
        return keys_;
    }
    common::ProtoMessage* GetMessage() { return message_; }
    int GetType() { return type_; }
    void SetWatcherId(WatcherId id) { watcher_id_ = id; }
    WatcherId GetWatcherId() { return watcher_id_; }
    int64_t GetExpireTime() { return expire_time_; }
    bool IsSentResponse() {
        std::lock_guard<std::mutex> lock(send_lock_);
        return sent_response_flag;
    }
    int64_t getKeyVersion() const {
        return key_version_;
    }
    int64_t GetSessionId() const{
        return session_id_;
    }

    int64_t GetMsgId() const {
        return msg_id_;
    }
public:
    virtual void Send(google::protobuf::Message* resp);

    static bool DecodeKey(std::vector<std::string*>& keys,
                   const std::string& buf);
    static bool DecodeValue(int64_t* version, std::string* value, std::string* extend,
                     std::string& buf);
    static void EncodeKey(std::string* buf,
                   uint64_t tableId, const std::vector<std::string*>& keys);
    static void EncodeValue(std::string* buf,
                     int64_t version,
                     const std::string* value,
                     const std::string* extend);

};

template <class T>
struct Greater {
    bool operator()(const T& a, const T& b) {
        return a->GetExpireTime() > b->GetExpireTime();
        //return a > b;
    }
};

/*
class KeyWatcher: public Watcher {
public:
    KeyWatcher() = delete;
    KeyWatcher(const Key&, common::ProtoMessage*);
    ~KeyWatcher() = default;

private:
    Key key_;

public:
    const Key& GetKey();
};

class PrefixWatcher: public Watcher {
public:
    PrefixWatcher() = delete;
    PrefixWatcher(const std::vector<Prefix>&, common::ProtoMessage*);
    ~PrefixWatcher() = default;

private:
    Prefix prefix_;

public:
    const Prefix& GetPrefix();
};
*/

typedef std::shared_ptr<Watcher>        WatcherPtr;
//typedef std::shared_ptr<KeyWatcher>     KeyWatcherPtr;
//typedef std::shared_ptr<PrefixWatcher>  PrefixWatcherPtr;


} // namespace watch
}
}

#endif
