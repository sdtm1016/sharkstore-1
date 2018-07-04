_Pragma("once");

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>

namespace sharkstore {
namespace raft {
namespace playground {

class Server;

class TelnetService {
public:
    TelnetService(Server* server, uint16_t port);
    ~TelnetService();

    TelnetService(const TelnetService&) = delete;
    TelnetService& operator=(const TelnetService&) = delete;

private:
    void do_accept();

private:
    Server* server_;

    asio::io_service io_service_;
    asio::ip::tcp::acceptor acceptor_;
    asio::ip::tcp::socket socket_;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */
