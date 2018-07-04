#include "fast_connection.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <atomic>

#include "base/util.h"
#include "common/ds_proto.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

FastConnection::~FastConnection() { this->Close(); }

Status FastConnection::Open(const std::string& ip, uint16_t port) {
    sockfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd_ < 0) {
        return Status(Status::kIOError, "new socket", strErrno(errno));
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    int ret = ::connect(sockfd_, (sockaddr*)&addr, sizeof(addr));
    if (ret < 0) {
        return Status(Status::kIOError, "connect", strErrno(errno));
    }

    // TODO: SO_SNDTIMEO

    return Status::OK();
}

Status FastConnection::Close() {
    if (sockfd_ > 0) {
        int ret = ::close(sockfd_);
        if (ret < 0) {
            return Status(Status::kIOError, "close socket", strErrno(errno));
        }
        sockfd_ = -1;
    }
    return Status::OK();
}

Status FastConnection::Send(MessagePtr& msg) {
    size_t body_len = msg->ByteSizeLong();
    size_t data_len = sizeof(ds_proto_header_t) + body_len;

    std::vector<char> buf_vec(data_len, '0');
    char* buf = buf_vec.data();

    ds_header_t header;
    memset(&header, 0, sizeof(header));
    header.magic_number = DS_PROTO_MAGIC_NUMBER;
    header.body_len = body_len;
    static std::atomic<uint64_t> msgid(1);
    header.msg_id = msgid.fetch_add(1);
    header.version = DS_PROTO_VERSION_CURRENT;
    header.msg_type = DS_PROTO_FID_RPC_RESP;
    header.func_id = 100;
    header.proto_type = 1;
    ds_serialize_header(&header, (ds_proto_header_t*)(buf));

    if (!msg->SerializeToArray(buf + sizeof(ds_proto_header_t), body_len)) {
        return Status(Status::kCorruption, "serialize snapshot msg",
                      "SerializeToArray return false");
    }

    int ret = ::send(sockfd_, buf, data_len, 0);
    if (ret <= 0) {
        return Status(Status::kIOError, "send to socket", strErrno(errno));
    }
    return Status::OK();
}

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
