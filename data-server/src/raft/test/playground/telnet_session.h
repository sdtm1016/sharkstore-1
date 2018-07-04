_Pragma("once");

#include <memory>

#include <asio/ip/tcp.hpp>
#include <asio/streambuf.hpp>

namespace sharkstore {
namespace raft {
namespace playground {

class Server;

class TelnetSession : public std::enable_shared_from_this<TelnetSession> {
public:
    TelnetSession(Server* s, asio::ip::tcp::socket socket);
    ~TelnetSession();

    TelnetSession(const TelnetSession&) = delete;
    TelnetSession& operator=(const TelnetSession&) = delete;

    void start();

private:
    void do_read();
    void do_write(const std::string& s);

private:
    Server* server_;
    asio::ip::tcp::socket socket_;
    asio::streambuf buffer_;
};

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */
