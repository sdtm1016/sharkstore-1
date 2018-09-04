#include "socket_session_impl.h"

#include <assert.h>

#include "frame/sf_logger.h"

#include "ds_proto.h"

namespace sharkstore {
namespace dataserver {
namespace common {

void SocketSessionImpl::Send(ProtoMessage *msg, google::protobuf::Message *resp) {
    // // 分配回应内存
    size_t body_len = resp == nullptr ? 0 : resp->ByteSizeLong();
    size_t data_len = header_size + body_len;

    response_buff_t *response = new_response_buff(data_len);

    // 填充应答头部
    ds_header_t header;
    header.magic_number = DS_PROTO_MAGIC_NUMBER;
    header.body_len = static_cast<int>(body_len);
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
    response->buff_len    = static_cast<int32_t>(data_len);

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

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
