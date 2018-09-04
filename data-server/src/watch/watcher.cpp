#include "watcher.h"
#include "common/socket_session_impl.h"
#include "common/ds_encoding.h"

#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

////////////////////////////////////// watcher //////////////////////////////////////

Watcher::Watcher(uint64_t table_id, const std::vector<WatcherKey*>& keys, const uint64_t &version, const int64_t &expire_time, common::ProtoMessage* msg):
        table_id_(table_id), key_version_(version), message_(msg), watcher_id_(msg->session_id), session_id_(msg->session_id), msg_id_(msg->header.msg_id), expire_time_(expire_time) {
    for (auto k: keys) {
        keys_.push_back(std::move(new WatcherKey(*k)));
    }
    if(keys.size() > 0) {
        keys_hash_.push_back(keys_[0]);
    }
}

Watcher::Watcher(WatchType type, uint64_t table_id, const std::vector<WatcherKey*>& keys, const uint64_t &version, const int64_t &expire_time, common::ProtoMessage* msg):
        table_id_(table_id), key_version_(version), message_(msg), type_(type), watcher_id_(msg->session_id),  session_id_(msg->session_id), msg_id_(msg->header.msg_id), expire_time_(expire_time) {
    for (auto k: keys) {
        keys_.push_back(std::move(new WatcherKey(*k)));
    }
    if(keys.size() > 0) {
        keys_hash_.push_back(keys_[0]);
    }
}

Watcher::Watcher(uint64_t table_id, const std::vector<WatcherKey*>& keys):table_id_(table_id) {
    for (auto k: keys) {
        keys_.push_back(std::move(new WatcherKey(*k)));
    }
    if(keys.size() > 0) {
        keys_hash_.push_back(keys_[0]);
    }
}

Watcher::~Watcher() {
    /*for (auto k: keys_) {
        delete k;
    }*/
}

bool Watcher::operator>(const Watcher* other) const {
    //return this->message_->expire_time > other->message_->expire_time;
    return this->expire_time_ > other->expire_time_;
}

void Watcher::Send(google::protobuf::Message* resp) {
    std::lock_guard<std::mutex> lock(send_lock_);
    if (sent_response_flag) {
        return;
    }

    uint32_t take_time = get_micro_second() - message_->begin_time;

    FLOG_DEBUG("before send, session_id: %" PRId64 ",task msgid: %" PRId64
               " execute take time: %d us",
               message_->session_id, message_->header.msg_id, take_time);


    common::SocketSessionImpl session;
    session.Send(message_, resp);

    message_ = nullptr;
    sent_response_flag = true;
}

bool Watcher::DecodeKey(std::vector<std::string*>& keys,
                       const std::string& buf) {
    assert(keys.size() == 0 && buf.length() > 9);

    size_t offset;
    for (offset = 9; offset < buf.length();) {
        std::string* b = new std::string();

        if (!DecodeBytesAscending(buf, offset, b)) {
            return false;
        }
        keys.push_back(b);
    }
    return true;
}

bool Watcher::DecodeValue(int64_t* version, std::string* value, std::string* extend,
                         std::string& buf) {
    assert(version != nullptr && value != nullptr && extend != nullptr &&
           buf.length() != 0);

    size_t offset = 0;
    if (!DecodeIntValue(buf, offset, version)) return false;
    if (!DecodeBytesValue(buf, offset, value)) return false;
    if (!DecodeBytesValue(buf, offset, extend)) return false;
    return true;
}

void Watcher::EncodeKey(std::string* buf,
               uint64_t tableId, const std::vector<std::string*>& keys) {
    assert(buf != nullptr && buf->length() == 0);
    assert(keys.size() != 0);

    buf->push_back(static_cast<char>(1));
    EncodeUint64Ascending(buf, tableId); // column 1
    assert(buf->length() == 9);

    for (auto key : keys) {
        EncodeBytesAscending(buf, key->c_str(), key->length());
    }
}

void Watcher::EncodeValue(std::string* buf,
                         int64_t version,
                         const std::string* value,
                         const std::string* extend) {
    assert(buf != nullptr);
    EncodeIntValue(buf, 2, version);
    EncodeBytesValue(buf, 3, value->c_str(), value->length());
    EncodeBytesValue(buf, 4, extend->c_str(), extend->length());
}

////////////////////////////////////// Key watcher //////////////////////////////////////
//KeyWatcher::KeyWatcher(uint64_t table_id, const Key& key, common::ProtoMessage* msg) {
//    std::vector<Key*> k;
//    k.push_back(&key);
//    Watcher::Watcher(k, msg);
//    EncodeKey(&key_, table_id, k);
//}

////////////////////////////////////// prefix watcher //////////////////////////////////////






} // namespace watch
}
}
