#include "socket_session_impl.h"

#include <assert.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "frame/sf_logger.h"

#include "ds_proto.h"

namespace sharkstore {
namespace dataserver {
namespace common {

// const int header_size = sizeof(ds_proto_header_t);

ProtoMessage *SocketSessionImpl::GetProtoMessage(const void *data) {
    assert(data != nullptr);

    ProtoMessage *msg = new ProtoMessage;
    if (msg == nullptr) return nullptr;
    // 解析头部
    ds_proto_header_t *proto_header = (ds_proto_header_t *)(data);

    ds_unserialize_header(proto_header, &(msg->header));

    //FLOG_INFO("new proto message. msgid: %" PRId64 ", func_id: %d, body_len: %d",
    //           msg->header.msg_id, msg->header.func_id, msg->header.body_len);

    // 拷贝数据
    if (msg->header.body_len > 0) {
        msg->body.resize(msg->header.body_len);
        memcpy(msg->body.data(), (char *)data + header_size, msg->body.size());
    }

    return msg;
}

bool SocketSessionImpl::GetMessage(const char *data, size_t size,
                                   google::protobuf::Message *req) {
    google::protobuf::io::ArrayInputStream input(data, size);
    return req->ParseFromZeroCopyStream(&input);
}

void SocketSessionImpl::Send(ProtoMessage *msg, google::protobuf::Message *resp) {
    // // 分配回应内存
    size_t body_len = resp == nullptr ? 0 : resp->ByteSizeLong();
    size_t data_len = header_size + body_len;

    response_buff_t *response = new_response_buff(data_len);

    // 填充应答头部
    ds_header_t header;
    header.magic_number = DS_PROTO_MAGIC_NUMBER;
    header.body_len = body_len;
    header.msg_id = msg->header.msg_id;
    header.version = DS_PROTO_VERSION_CURRENT;
    header.msg_type = DS_PROTO_FID_RPC_RESP;
    header.func_id = msg->header.func_id;
    header.proto_type = msg->header.proto_type;

    ds_serialize_header(&header, (ds_proto_header_t *)(response->buff));

    response->session_id  = msg->session_id;
    response->msg_id      = header.msg_id;
    response->begin_time  = msg->begin_time;
    response->expire_time = msg->expire_time;
    response->buff_len    = data_len;

    do {
        if (resp != nullptr) {
            char *data = response->buff + header_size;
            if (!resp->SerializeToArray(data, body_len)) {
                FLOG_ERROR("serialize response failed, func_id: %d", header.func_id);
                delete_response_buff(response);
                break;
            }
        }

        //处理完成，socket send
        msg->socket->Send(response);

    } while (false);

    delete msg;
    delete resp;
}

void SocketSessionImpl::SetResponseHeader(const kvrpcpb::RequestHeader &req,
                                          kvrpcpb::ResponseHeader *resp,
                                          errorpb::Error *err) {
    // TODO
    // set timestamp
    resp->set_cluster_id(req.cluster_id());
    resp->set_trace_id(req.trace_id());

    if (err != nullptr) {
        resp->set_allocated_error(err);
    }
}

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
