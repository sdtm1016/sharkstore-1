#include "fast_client.h"

#include <mutex>
#include "common/ds_proto.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

FastClient::FastClient(const sf_socket_thread_config_t &cfg,
                       const std::shared_ptr<NodeResolver> &resolver)
    : config_(cfg), resolver_(resolver), msg_id_(1) {
    memset(&status_, 0, sizeof(status_));
}

FastClient::~FastClient() {
    // TODO:
}

static void client_recv_done(request_buff_t *request, void *args) {
    // TODO:
}

Status FastClient::Initialize() {
    int ret = dataserver::common::SocketBase::Init(&config_, &status_);
    if (ret != 0) {
        return Status(Status::kUnknown, "init raft fast client",
                      std::string("ret: ") + std::to_string(ret));
    }

    thread_info_.recv_callback = client_recv_done;
    thread_info_.user_data = this;
    thread_info_.is_client = true;

    ret = dataserver::common::SocketBase::Start();
    if (ret != 0) {
        return Status(Status::kUnknown, "start raft fast client",
                      std::string("ret: ") + std::to_string(ret));
    }
    return Status::OK();
}

void FastClient::Shutdown() { dataserver::common::SocketBase::Stop(); }

void FastClient::SendMessage(MessagePtr &msg) {
    if (msg->to() == 0) {
        FLOG_ERROR(
            "raft[FastClient] invalid target node(0), type: %s, id: %lu, term: %lu",
            pb::MessageType_Name(msg->type()).c_str(), msg->id(), msg->term());
    }

    int64_t sid = getSession(msg->to());
    if (sid > 0) {
        send(sid, msg);
    } else {
        FLOG_ERROR("raft[FastClient] could not get a connection to %lu", msg->to());
    }
}

int64_t FastClient::getSession(uint64_t to) {
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> locker(mu_);
        auto it = sessions_.find(to);
        if (it != sessions_.end()) {
            return it->second;
        }
    }

    // get remote ip and port
    std::string ip;
    int port = 0;
    std::string addr = resolver_->GetNodeAddress(to);
    if (addr.empty()) {
        return 0;
    }
    auto pos = addr.find(':');
    if (pos != std::string::npos) {
        ip = addr.substr(0, pos);
        port = atoi(addr.substr(pos + 1).c_str());
    } else {
        return 0;
    }

    std::unique_lock<sharkstore::shared_mutex> locker(mu_);
    auto it = sessions_.find(to);
    if (it != sessions_.end()) {
        return it->second;
    }
    auto id = sf_connect_session_get(&thread_info_, ip.c_str(), port);
    if (id > 0) {
        FLOG_INFO("raft[FastClient] connect to %s:%d success. sid=%ld", ip.c_str(), port,
                  id);
        sessions_.emplace(to, id);
    } else {
        FLOG_ERROR("raft[FastClient] connect failed to %s:%d", ip.c_str(), port);
    }
    return id;
}

void FastClient::removeSession(uint64_t to) {
    std::unique_lock<sharkstore::shared_mutex> locker(mu_);
    sessions_.erase(to);
}

void FastClient::send(int64_t sid, MessagePtr &msg) {
    size_t body_len = msg->ByteSizeLong();
    size_t data_len = sizeof(ds_proto_header_t) + body_len;

    response_buff_t *response = new_response_buff(data_len);

    // 填充头部
    ds_header_t header;
    header.magic_number = DS_PROTO_MAGIC_NUMBER;
    header.body_len = body_len;
    header.msg_id = msg_id_.fetch_add(1);
    header.version = DS_PROTO_VERSION_CURRENT;
    header.msg_type = DS_PROTO_FID_RPC_RESP;
    header.func_id = 100;
    header.proto_type = 1;
    ds_serialize_header(&header, (ds_proto_header_t *)(response->buff));

    response->session_id = sid;
    response->buff_len = data_len;

    if (msg->SerializeToArray(response->buff + sizeof(ds_proto_header_t), body_len)) {
        int ret = dataserver::common::SocketBase::Send(response);
        if (ret != 0) {
            FLOG_ERROR("raft[FastClient] send to %lu failed. ret=%d, sid=%ld", msg->to(),
                       ret, sid);
            removeSession(msg->to());
        }
    } else {
        delete_response_buff(response);
    }
}

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */