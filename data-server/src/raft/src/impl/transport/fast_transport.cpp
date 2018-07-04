#include "fast_transport.h"

#include "fast_client.h"
#include "fast_connection.h"
#include "fast_server.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

FastTransport::FastTransport(const std::shared_ptr<NodeResolver>& resolver,
                             size_t send_threads, size_t recv_threads)
    : resolver_(resolver),
      recv_threads_num_(recv_threads) {}

FastTransport::~FastTransport() {
    delete server_;
    delete client_;
}

Status FastTransport::Start(const std::string& listen_ip, uint16_t listen_port,
                            const MessageHandler& handler) {
    // new server
    sf_socket_thread_config_t srv_config;
    memset(&srv_config, 0, sizeof(srv_config));
    srv_config.accept_threads = 1;
    srv_config.event_recv_threads = recv_threads_num_;
    srv_config.recv_buff_size = 128 * 1024;

    const char* ip = listen_ip.empty() ? "0.0.0.0" : listen_ip.c_str();
    strncpy(srv_config.ip_addr, ip, strlen(ip));
    srv_config.port = listen_port;

    strcpy(srv_config.thread_name_prefix, "raft");
    server_ = new FastServer(srv_config, handler);

    // new client
    sf_socket_thread_config_t cli_config;
    memset(&cli_config, 0, sizeof(cli_config));
    cli_config.event_send_threads = 1;
    strcpy(cli_config.thread_name_prefix, "raft");
    client_ = new FastClient(cli_config, resolver_);

    auto s = server_->Initialize();
    if (!s.ok()) {
        return s;
    }
    return client_->Initialize();
}

void FastTransport::Shutdown() {
    server_->Shutdown();
    client_->Shutdown();
}

void FastTransport::SendMessage(MessagePtr& msg) { client_->SendMessage(msg); }

Status FastTransport::GetConnection(uint64_t to,
                                    std::shared_ptr<Connection>* conn) {
    std::string ip;
    uint16_t port = 0;
    std::string addr = resolver_->GetNodeAddress(to);
    auto pos = addr.find(':');
    if (pos != std::string::npos) {
        ip.assign(addr.substr(0, pos));
        port = atoi(addr.substr(pos + 1).c_str());
    } else {
        return Status(Status::kInvalidArgument, "resolve node address",
                      std::to_string(to));
    }

    auto c = std::make_shared<FastConnection>();
    auto s = c->Open(ip, port);
    if (!s.ok()) return s;

    *conn = std::static_pointer_cast<Connection>(c);
    return Status::OK();
}

} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
