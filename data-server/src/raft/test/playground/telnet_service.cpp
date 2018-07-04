#include "telnet_service.h"
#include "telnet_session.h"

#include <iostream>
#include <thread>
#include "server.h"

namespace sharkstore {
namespace raft {
namespace playground {

using namespace asio::ip;

TelnetService::TelnetService(Server* server, uint16_t port)
    : server_(server),
      acceptor_(io_service_, tcp::endpoint(tcp::v4(), port)),
      socket_(io_service_) {
    if (port == 0) {
        std::cerr << "ERR: invalid telnet port " << port << std::endl;
        exit(EXIT_FAILURE);
    }
    do_accept();
    std::thread t([this]() {
        try {
            io_service_.run();
        } catch (std::exception& e) {
            std::cerr << "ERR: asio service exception: " << e.what()
                      << std::endl;
            exit(EXIT_FAILURE);
        }
    });
    t.detach();

    std::cout << "start telnet service on :" << port << std::endl;
}

TelnetService::~TelnetService() {}

void TelnetService::do_accept() {
    acceptor_.async_accept(socket_, [this](std::error_code ec) {
        if (!ec) {
            std::make_shared<TelnetSession>(server_, std::move(socket_))
                ->start();
        } else {
            std::cout << "ERR: accept telent connection failed: "
                      << ec.message() << std::endl;
            exit(EXIT_FAILURE);
        }
        do_accept();
    });
}

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */