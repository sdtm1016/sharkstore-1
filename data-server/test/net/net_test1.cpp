#include <glog/logging.h>
#include <asio/steady_timer.hpp>
#include <iostream>

#include "frame/sf_logger.h"
#include "net/server.h"

using namespace sharkstore::dataserver::net;

void onRPC(const MsgContext &, const RPCHead &, std::vector<uint8_t> &&) {
    FLOG_INFO("recv rpc request");
}
void onTelnet(const MsgContext &, std::string &&cmdline) {
    FLOG_INFO("recv telnet cmdline: %s", cmdline.c_str());
}

int main(int argc, char *argv[]) {
    log_set_prefix(".", "net_test1");
    log_init2();
    g_log_context.log_level = LOG_DEBUG;

    RPCHead head;
    FLOG_INFO("head: %s", head.DebugString().c_str());

    Server server{ServerOptions()};

    MsgHandler handler;
    handler.rpc_handler = onRPC;
    handler.telnet_handler = onTelnet;

    auto s = server.ListenAndServe("127.0.0.1", 9999, handler);
    if (!s.ok()) {
        FLOG_ERROR("start net server failed: %s", s.ToString().c_str());
        return -1;
    }

    pause();

    return 0;
}
