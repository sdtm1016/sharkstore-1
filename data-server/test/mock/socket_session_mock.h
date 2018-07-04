#ifndef __SOCKET_SESSION_MOCK_H__
#define __SOCKET_SESSION_MOCK_H__

#include <string>

#include "common/socket_session.h"

using namespace sharkstore;
using namespace sharkstore::dataserver::common;

class SocketSessionMock : public SocketSession {
public:
    SocketSessionMock() = default;
    ~SocketSessionMock() = default;

    SocketSessionMock(const SocketSessionMock &) = delete;
    SocketSessionMock &operator=(const SocketSessionMock &) = delete;

    ProtoMessage *GetProtoMessage(const void *data) override;

    void Send(ProtoMessage *buff, google::protobuf::Message *resp) override;

    bool GetMessage(const char *data, size_t size,
                    google::protobuf::Message *req) override;

    void SetResponseHeader(const kvrpcpb::RequestHeader &req,
                           kvrpcpb::ResponseHeader *resp,
                           errorpb::Error *err = nullptr) override;

    bool GetResult(google::protobuf::Message *req);

private:
    std::string result_;
};

#endif  //__SOCKET_SESSION_MOCK_H__
