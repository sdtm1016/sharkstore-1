#ifndef __SOCKET_SESSION_IMPL_H__
#define __SOCKET_SESSION_IMPL_H__

#include "socket_session.h"

namespace sharkstore {
namespace dataserver {
namespace common {


class SocketSessionImpl : public SocketSession {
public:
    SocketSessionImpl() = default;
    ~SocketSessionImpl() = default;

    SocketSessionImpl(const SocketSessionImpl&) = delete;
    SocketSessionImpl& operator=(const SocketSessionImpl&) = delete;


    ProtoMessage *GetProtoMessage(const void *data) override;

    void Send(ProtoMessage *msg, google::protobuf::Message* resp) override;

    bool GetMessage(const char *data, size_t size,
            google::protobuf::Message* req) override;

    void SetResponseHeader(const kvrpcpb::RequestHeader &req,
            kvrpcpb::ResponseHeader *resp,  errorpb::Error *err = nullptr) override;

};

} //namespace common
} //namespace dataserver
} //namespace sharkstore

#endif //__SOCKET_SESSION_IMPL_H__
