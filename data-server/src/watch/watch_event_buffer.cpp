//
// Created by zhangyongcheng on 18-8-7.
//

#include "watch_event_buffer.h"



namespace sharkstore {
namespace dataserver {
namespace watch {

void printBufferValue(CEventBufferValue &val) {
    FLOG_DEBUG("key:%s value:%s version:%"
                       PRId64,
               EncodeToHexString(val.key(0)).c_str(), EncodeToHexString(val.value()).c_str(),
               val.version());
}

bool operator < (const struct SGroupKey &l, const struct SGroupKey &r) {
    return l.key_ < r.key_;
}

bool CEventBuffer::thread_flag_=true;
int32_t CEventBuffer::milli_timeout_ = EVENT_BUFFER_TIME_OUT;

CEventBuffer::CEventBuffer() {
    mapGroupBuffer.clear();
//    create_thread();
}

CEventBuffer::CEventBuffer(const int &mapSize, const int &queueSize) {
    map_capacity_ = mapSize>MAX_EVENT_BUFFER_MAP_SIZE?MAX_EVENT_BUFFER_MAP_SIZE:mapSize;
    queue_capacity_ = queueSize>MAX_EVENT_QUEUE_SIZE?MAX_EVENT_QUEUE_SIZE:queueSize;

    map_capacity_ = map_capacity_>0?map_capacity_:DEFAULT_EVENT_BUFFER_MAP_SIZE;
    queue_capacity_ = queue_capacity_>0?queue_capacity_:DEFAULT_EVENT_QUEUE_SIZE;

    mapGroupBuffer.clear();
//    create_thread();
}

CEventBuffer::~CEventBuffer() {
    for(auto it : mapGroupBuffer) {
        delete it.second;
    }
    loop_flag_ = false;
//    clear_thread_.join();
}

BufferReturnPair  CEventBuffer::loadFromBuffer(const std::string &grpKey,  int64_t userVersion,
                                 std::vector<CEventBufferValue> &result) {

    std::lock_guard<std::mutex> lock(buffer_mutex_);

    int32_t resultCnt{0};
    BufferReturnPair retPair = std::make_pair(-1, std::make_pair(0,0));

    if(isEmpty() || userVersion == 0) {
        return retPair;
    }

    int32_t from(0), to(0);

    auto it = mapGroupBuffer.find(grpKey);
    if(it != mapGroupBuffer.end()) {
        //to do 遍历获取版本范围内变更
        resultCnt = it->second->getData(userVersion, result);

        from = it->second->lowerVersion();
        to = it->second->upperVersion();
    }

    retPair = std::make_pair(resultCnt, std::make_pair(from, to));
    return retPair;
}

bool CEventBuffer::enQueue(const std::string &grpKey, const CEventBufferValue *bufferValue) {

    std::lock_guard<std::mutex> lock(buffer_mutex_);
    bool ret{false};
    int64_t queueLength(0);
    GroupKey key(grpKey);

    auto it = mapGroupBuffer.find(key);
    if(it == mapGroupBuffer.end()) {

        //to do escasp from map
        if(isFull()) {
            GroupKey k(listGroupBuffer.begin()->key_, listGroupBuffer.begin()->create_time_);
            listGroupBuffer.pop_front();

            FLOG_INFO("buffer_map auto pop key:%s", EncodeToHexString(k.key_).c_str());

            auto itMap = mapGroupBuffer.find(k);
            if(itMap != mapGroupBuffer.end()) {
                deQueue(itMap->second);
                mapGroupBuffer.erase(itMap);
                map_size_--;
                FLOG_INFO("map pop success, key:%s  create(ms):%" PRId64 " map-length:%" PRId32, k.key_.c_str(), k.create_time_, map_size_);
            } else {
                FLOG_INFO("map pop error, key:%s  create(ms):%" PRId64 " map-length:%" PRId32, k.key_.c_str(), k.create_time_, map_size_);
            }
        }

        auto grpValue = new GroupValue(queue_capacity_);

        if(grpValue->enQueue(*bufferValue)) {
            listGroupBuffer.push_back(key);
            auto result = mapGroupBuffer.emplace(std::make_pair(key, grpValue));
            if(result.second) {
                ret = true;
            } else {
                FLOG_INFO("mapGroupBuffer emplace error, key:%s", EncodeToHexString(grpKey).c_str());
                ret = false;
            }
            queueLength = grpValue->length();

            if(ret)
                map_size_++;

        } else {
            FLOG_WARN("map[%s]->queue is full[%" PRId32 "]", EncodeToHexString(grpKey).c_str(), queue_capacity_);
            ret = false;
        }
    } else {
        ret = it->second->enQueue(*bufferValue);
        if(!ret) {
            FLOG_WARN("map[%s]->queue is full..", EncodeToHexString(grpKey).c_str());
        }
        queueLength = it->second->length();
    }

    FLOG_DEBUG("capacity:%" PRId32 "-%" PRId32 " >>>emplace to queue, key:%s value:%s version:%"
                                               PRId64 " map-length:%" PRId32 " queue-length:%" PRId64,
               map_capacity_, queue_capacity_, EncodeToHexString(grpKey).c_str(), bufferValue->value().c_str(),
               bufferValue->version(), map_size_, queueLength);

    return ret;
}

bool CEventBuffer::deQueue(GroupValue   *grpVal) {

    //std::lock_guard<std::mutex> lock(buffer_mutex_);
    if(grpVal != nullptr) {
        grpVal->clearQueue();
        delete grpVal;
    }

    return true;
}


}
}
}