_Pragma("once");

namespace sharkstore {
namespace dataserver {
namespace net {

struct SessionOptions {
    // connection read timeout
    // a zero value means no timeout
    size_t read_timeout_ms = 5000;

    // connection write request timeout
    size_t write_timeout_ms = 5000;

    // max pending writes per connection
    // TODO: limit memory usage
    size_t write_queue_capacity = 2000;

    // allowed max packet length when read
    size_t max_packet_length = 10 << 20;
};

struct ServerOptions {
    // how many threads will server connections use
    // zero value means share with the accept thread
    size_t io_threads_num = 4;

    // exceeded connections will be rejected
    size_t max_connections = 50000;

    // options about session
    SessionOptions session_opt;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
