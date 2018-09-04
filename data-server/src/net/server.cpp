#include "server.h"

#include "frame/sf_logger.h"

#include "io_context_pool.h"
#include "session.h"

namespace sharkstore {
namespace dataserver {
namespace net {

Server::Server(const ServerOptions& opt)
    : opt_(opt),
      acceptor_(context_),
      context_pool_(new IOContextPool(opt.io_threads_num)) {}

Server::~Server() { Stop(); }

Status Server::ListenAndServe(const std::string& listen_ip, uint16_t listen_port,
                              const Handler& handler) {
    std::string bind_ip = listen_ip;
    if (bind_ip.empty()) {
        bind_ip = "0.0.0.0";
    }
    try {
        asio::ip::tcp::endpoint endpoint(asio::ip::make_address(bind_ip), listen_port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen(asio::socket_base::max_listen_connections);
    } catch (std::exception& e) {
        return Status(Status::kIOError, "listen", e.what());
    }

    handler_ = handler;
    context_pool_->Start();

    doAccept();
    thr_.reset(new std::thread([this]() {
        try {
            context_.run();
        } catch (...) {
        }
    }));
    thr_->detach();

    return Status::OK();
}

void Server::Stop() {
    if (stopped_) return;

    stopped_ = true;

    acceptor_.close();
    context_.stop();
    context_pool_->Stop();
}

void Server::doAccept() {
    acceptor_.async_accept(getContext(), [this](const std::error_code& ec,
                                                asio::ip::tcp::socket socket) {
        if (ec) {
            FLOG_ERROR("[Net] accept error: %s", ec.message().c_str());
        } else if (Session::TotalCount() > opt_.max_connections) {
            FLOG_WARN("[Net] accept max connection limit reached: %lu",
                      opt_.max_connections);
        } else {
            std::make_shared<Session>(opt_.session_opt, handler_, std::move(socket))
                ->Start();
        }

        doAccept();
    });
}

asio::io_context& Server::getContext() {
    if (context_pool_->Size() > 0) {
        return context_pool_->GetIOContext();
    } else {
        return context_;
    }
}

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
