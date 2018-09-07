#include <unistd.h>
#include <iostream>

#include "bench.h"

#include "proto/gen/kvrpcpb.pb.h"
#include "frame/sf_logger.h"
#include "net/server.h"
#include "net/session.h"

using namespace sharkstore::dataserver::net;

void handle(const Context& ctx, const MessagePtr& msg) {
    if (msg->head.func_id != kBenchFuncID) {
        std::cerr << "Invalid func id: " << msg->head.func_id << std::endl;
        exit(EXIT_FAILURE);
    }

    kvrpcpb::DsKvRawPutResponse resp;
    auto resp_msg = NewMessage();
    resp_msg->head.SetResp(msg->head);
    resp_msg->body.resize(resp.ByteSizeLong());
    resp.SerializeToArray(resp_msg->body.data(), static_cast<int>(resp_msg->body.size()));
    auto conn = ctx.session.lock();
    if (conn) {
        conn->Write(resp_msg);
    }
}

int main(int argc, char *argv[]) {
    int opt = 0, nthreads = 4;
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
            case 't':
                nthreads = atoi(optarg);
                break;
            default: /* '?' */
                std::cerr << "Usage: " << argv[0] << " [-t threads_num]" << std::endl;
                exit(EXIT_FAILURE);
        }
    }
    std::cout << "IO Threads: " << nthreads << std::endl;

    log_set_prefix(".", "asio_bench_server");
    log_init2();
    g_log_context.log_level = LOG_INFO;

    ServerOptions sops;
    sops.io_threads_num = static_cast<size_t>(nthreads);
    Server server(sops);
    auto s = server.ListenAndServe("0.0.0.0", kBenchServerPort, handle);
    if (!s.ok()) {
        std::cerr << "listen on " << kBenchServerPort << " failed: " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }

    pause();

    return 0;
}
