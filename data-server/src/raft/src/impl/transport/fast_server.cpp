#include "fast_server.h"

#include "common/ds_proto.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

FastServer::FastServer(const sf_socket_thread_config_t& config,
                       const MessageHandler& handler)
    : config_(config), handler_(handler) {
    memset(&status_, 0, sizeof(status_));
}

FastServer::~FastServer() {}

void fastserver_recv_task_cb(request_buff_t* task, void* args) {
    FastServer* s = static_cast<FastServer*>(args);
    s->handleTask(task);
}

void fastserver_send_done_cb(response_buff_t* task, void* args, int err) {
    FastServer* s = static_cast<FastServer*>(args);
    s->sendDoneCallback(task, err);
}

Status FastServer::Initialize() {
    int ret = dataserver::common::SocketBase::Init(&config_, &status_);
    if (ret != 0) {
        return Status(Status::kUnknown, "init raft fast server",
                      std::string("ret: ") + std::to_string(ret));
    }

    thread_info_.is_client = false;
    thread_info_.user_data = this;
    thread_info_.recv_callback = fastserver_recv_task_cb;
    thread_info_.send_callback = fastserver_send_done_cb;

    ret = dataserver::common::SocketBase::Start();
    if (ret != 0) {
        return Status(Status::kUnknown, "start raft fast server",
                      std::string("ret: ") + std::to_string(ret));
    }

    return Status::OK();
}

void FastServer::Shutdown() { dataserver::common::SocketBase::Stop(); }

void FastServer::handleTask(request_buff_t* task) {
    // 解析头部
    ds_header_t header;
    memset(&header, 0, sizeof(header));
    ds_proto_header_t* proto_header = (ds_proto_header_t*)(task->buff);
    ds_unserialize_header(proto_header, &header);

    if (header.body_len > 0) {
        MessagePtr msg(new pb::Message);
        bool ret =
            msg->ParseFromArray(task->buff + sizeof(ds_proto_header_t), header.body_len);
        if (ret) {
            handler_(msg);
        } else {
            FLOG_ERROR("raft[FastServer] prase protobuf message failed.");
        }
    }
}

void FastServer::sendDoneCallback(response_buff_t* task, int err) {
    // TODO: log
}

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */