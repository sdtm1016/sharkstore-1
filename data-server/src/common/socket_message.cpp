#include "socket_message.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace sharkstore {
namespace dataserver {
namespace common {

ProtoMessage *GetProtoMessage(const void *data) {
    auto msg = new ProtoMessage;

    // 解析头部
    auto proto_header = (ds_proto_header_t *)(data);
    ds_unserialize_header(proto_header, &(msg->header));

    // 拷贝数据
    if (msg->header.body_len > 0) {
        msg->body.resize(static_cast<size_t>(msg->header.body_len));
        memcpy(msg->body.data(), (char *)data + header_size, msg->body.size());
    }
    return msg;
}

bool GetMessage(const char *data, size_t size,
                google::protobuf::Message *req, bool zero_copy) {
    if (zero_copy) {
        google::protobuf::io::ArrayInputStream input(data, static_cast<int>(size));
        return req->ParseFromZeroCopyStream(&input);
    } else {
        return req->ParseFromArray(data, static_cast<int>(size));
    }
}

void SetResponseHeader(const kvrpcpb::RequestHeader &req,
                       kvrpcpb::ResponseHeader *resp,
                       errorpb::Error *err) {
    resp->set_cluster_id(req.cluster_id());
    resp->set_trace_id(req.trace_id());
    if (err != nullptr) {
        resp->set_allocated_error(err);
    }
}

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
