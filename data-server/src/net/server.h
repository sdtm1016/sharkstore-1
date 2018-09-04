_Pragma("once");

#include <thread>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include "base/status.h"

#include "handler.h"
#include "options.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class IOContextPool;

class Server final {
public:
    explicit Server(const ServerOptions& opt);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    Status ListenAndServe(const std::string& listen_ip, uint16_t listen_port,
                          const Handler &handler);

    void Stop();

private:
    void doAccept();
    asio::io_context& getContext();

private:
    const ServerOptions opt_;
    Handler handler_;

    bool stopped_ = false;

    // acceptor
    asio::io_context context_;
    asio::ip::tcp::acceptor acceptor_;

    std::unique_ptr<IOContextPool> context_pool_;

    std::unique_ptr<std::thread> thr_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
