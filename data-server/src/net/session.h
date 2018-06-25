_Pragma("once");

#include <memory>
#include <queue>
#include <asio/ip/tcp.hpp>
#include <asio/streambuf.hpp>
#include <asio/steady_timer.hpp>

#include "msg_handler.h"
#include "options.h"
#include "rpc_protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(const SessionOptions& opt, const MsgHandler& msg_handler,
            asio::ip::tcp::socket socket);
    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    void Connect(const std::string& addr);
    void Start();
    void Close();

    void Write(std::string&& data);
    void Write(const RPCHead& header, std::vector<uint8_t>&& body);

    static uint64_t TotalCount() { return total_count_; }

private:
    // all server's sessions count
    static std::atomic<uint64_t> total_count_;

private:
    bool init();
    void doClose();

    void resetReadTimer();

    void readPreface();

    void readRPCHead();
    void readRPCBody();

    void readCmdLine();
    void parseCmdLine(std::size_t length);

private:
    const SessionOptions& opt_;
    const MsgHandler& msg_handler_;

    asio::ip::tcp::socket socket_;
    MsgContext msg_ctx_;
    std::string id_;

    std::atomic<bool> closed_ = {false};

    // read
    std::array<uint8_t, 4> preface_ = {{0, 0, 0, 0}};
    size_t preface_remained_ = 4;
    RPCHead rpc_head_;
    std::vector<uint8_t> rpc_body_;
    asio::streambuf cmdline_buffer_;
    asio::steady_timer read_timeout_timer_;

    // write
    std::queue<int> write_queue_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
