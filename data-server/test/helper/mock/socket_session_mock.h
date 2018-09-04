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

    void Send(ProtoMessage *buff, google::protobuf::Message *resp) override;

    bool GetResult(google::protobuf::Message *req);

private:
    bool pending_ = false;
    std::string result_;
};

#endif  //__SOCKET_SESSION_MOCK_H__
