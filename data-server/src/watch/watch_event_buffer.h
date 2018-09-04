//
// Created by zhangyongcheng on 18-8-7.
//
_Pragma("once");

#include "circular_queue.h"
#include "proto/gen/watchpb.pb.h"
#include "frame/sf_logger.h"
#include "common/ds_encoding.h"
#include "frame/sf_util.h"

#include <list>
#include <mutex>
#include <thread>
#include <condition_variable>

namespace sharkstore {
namespace dataserver {
namespace watch {

//ms
#define EVENT_BUFFER_TIME_OUT 300000
#define MAX_EVENT_BUFFER_MAP_SIZE 10000
#define MAX_EVENT_QUEUE_SIZE  100000
#define DEFAULT_EVENT_BUFFER_MAP_SIZE 10
#define DEFAULT_EVENT_QUEUE_SIZE  100

class CEventBufferValue;
void printBufferValue(CEventBufferValue &val);

using BufferReturnPair = std::pair<int32_t, std::pair<int32_t, int32_t>>;

class CEventBufferValue {
public:
    CEventBufferValue() = default;

    CEventBufferValue(const CEventBufferValue &oth){
        *this = oth;
    };

    CEventBufferValue &operator=(const CEventBufferValue &oth) {
        this->key_.assign(oth.key_.begin(), oth.key_.end());
        this->value_ = oth.value_;
        this->version_ = oth.version_;
        this->evtType_ = oth.evtType_;
        setUpdateTime();

        //std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>version:" << oth.version_ << std::endl;
        return *this;
    }

    CEventBufferValue(const watchpb::WatchKeyValue &val, const watchpb::EventType &evtType, const int64_t &keyVersion) {
        CopyFrom(val);
        this->evtType_ = evtType;
        this->version_ = keyVersion;
        setUpdateTime();
    }

    const int64_t &version() const {
        return version_;
    }
    int32_t key_size() const {
        return key_.size();
    }
    const std::vector<std::string> &key() const {
        return key_;
    }
    const std::string &key(uint32_t idx) const {
        assert(key_.size() > idx);
        return key_[idx];
    }
    const std::string &value() const {
        return value_;
    }
    const watchpb::EventType &type() const {
        return evtType_;
    }

    int64_t usedTime() const {
        return used_time_;
    }

    void setUpdateTime() {
        used_time_ = getticks();
    }

private:
    void CopyFrom(const watchpb::WatchKeyValue &val) {
        for(auto it : val.key()) {
            key_.emplace_back(it);
        }
        value_ = val.value();
        version_ = val.version();
    }

private:
    watchpb::EventType evtType_{watchpb::PUT};
    std::vector<std::string> key_;
    std::string value_;
    int64_t version_;
    int64_t used_time_;
};

using GroupKey = struct SGroupKey {
    std::string key_;
    int64_t create_time_;

    SGroupKey(const std::string &key) {
        key_ = key;
        create_time_ = getticks();
    }

    SGroupKey(const std::string &key, const int64_t &time) {
        key_ = key;
        create_time_ = time;
    }
};
bool operator < (const struct SGroupKey &l, const struct SGroupKey &r);

using GroupValue = CircularQueue<CEventBufferValue>;
using MapGroupBuffer = std::map<GroupKey, GroupValue *>;
using ListGroupBuffer = std::list<GroupKey>;

class CEventBuffer {
public:
    CEventBuffer();
    CEventBuffer(const int &mapSize, const int &queueSize);
    ~CEventBuffer();

    //<hit cnt:version scope in buffer<from:to> >
    BufferReturnPair loadFromBuffer(const std::string &grpKey, int64_t uerVersion, std::vector<CEventBufferValue> &result);

    bool enQueue(const std::string &grpKey, const CEventBufferValue *bufferValue);

    bool deQueue(GroupValue   *grpVal);

    void clear(const std::string &grpKey) {
        GroupKey gk(grpKey);

        auto it = mapGroupBuffer.find(gk);
        if (it != mapGroupBuffer.end()) {
            it->second->clearQueue();
            mapGroupBuffer.erase(it);
        }
    }

    bool isEmpty() const {
        return (mapGroupBuffer.size() == 0);
    }

    bool isFull() const {
        return (mapGroupBuffer.size() == MAX_EVENT_BUFFER_MAP_SIZE);
    }




private:
    MapGroupBuffer mapGroupBuffer;
    ListGroupBuffer listGroupBuffer;

    int32_t map_capacity_{DEFAULT_EVENT_BUFFER_MAP_SIZE};
    int32_t queue_capacity_{DEFAULT_EVENT_QUEUE_SIZE};
    int32_t map_size_{0};

    std::mutex buffer_mutex_;
    std::condition_variable buffer_cond_;

    static int32_t milli_timeout_;
    static bool thread_flag_;
    std::thread clear_thread_;
    volatile bool loop_flag_{true};

};


}
}
}

