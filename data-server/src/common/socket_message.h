_Pragma("once");

#include <vector>
#include <google/protobuf/message.h>

#include "proto/gen/errorpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "socket_base.h"
#include "ds_proto.h"
#include "frame/sf_util.h"

namespace sharkstore {
namespace dataserver {
namespace common {

struct ProtoMessage {
    int64_t session_id = 0;
    int64_t begin_time = 0;
    int64_t expire_time = 0;
    int64_t msg_id = 0;
    ds_header_t header;
    SocketBase *socket = nullptr;
    std::vector<char> body;

    ProtoMessage(){};
    explicit ProtoMessage(int64_t expire): expire_time(getticks()+expire) {};
    virtual ~ProtoMessage(){};
    ProtoMessage(const struct ProtoMessage &other) {
        this->session_id = other.session_id;
        this->begin_time = other.begin_time;
        this->expire_time = other.expire_time;
        this->msg_id = other.msg_id;
        this->header = other.header;
        this->socket = other.socket;
        this->body.assign(other.body.begin(), other.body.end());
    }

};

// 从报文数据中解析生成ProtoMessage
ProtoMessage *GetProtoMessage(const void *data);

// 反序列化
bool GetMessage(const char *data, size_t size,
        google::protobuf::Message *req, bool zero_copy = true);

// 设置ResponseHeader字段
void SetResponseHeader(const kvrpcpb::RequestHeader &req,
        kvrpcpb::ResponseHeader *resp,
        errorpb::Error *err = nullptr);

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
