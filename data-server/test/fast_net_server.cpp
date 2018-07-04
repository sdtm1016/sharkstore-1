#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <map>
#include <iostream>

#include <fastcommon/logger.h>

#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "common/ds_types.h"
#include "common/ds_version.h"
#include "common/socket_server.h"
#include "common/socket_session_impl.h"
#include "frame/sf_service.h"
#include "frame/sf_socket_buff.h"
#include "frame/sf_status.h"
#include "frame/sf_util.h"
#include "frame/sf_logger.h"

#include "./gen/test.pb.h"

sf_socket_thread_config_t config;
sf_socket_status_t status = {0};

using namespace sharkstore::dataserver::common;

SocketSession *socket_session = new SocketSessionImpl;
SocketServer socket_server;

std::map<int64_t, uint64_t> qps;
std::mutex qps_mutex;

static int load_conf_file(IniContext *ini_context, const char *filename) {
    return sf_load_socket_thread_config(ini_context, "worker", &config);
}

void send(ProtoMessage *msg) {
    tpb::TestMsg req;
    if (!socket_session->GetMessage(msg->body.data(), msg->body.size(), &req)) {
        FLOG_ERROR("deserialize create range request failed");
        return socket_session->Send(msg, nullptr);
    }

    //FLOG_INFO("%s", req.message().c_str());
    tpb::TestMsg *resp = new tpb::TestMsg;
    resp->set_message("this is fast net test!!!");

    socket_session->Send(msg, resp);
}

void deal(request_buff_t *request, void *args) {
    ProtoMessage *req = socket_session->GetProtoMessage(request->buff);
    if (req != nullptr) {
        req->socket = &socket_server;
        req->session_id = request->session_id;
        req->expire_time = getticks();

        if (req->header.time_out > 0) {
            req->expire_time += req->header.time_out;
        } else {
            req->expire_time += ds_config.task_timeout;
        }

        //auto t = time(NULL);
        //std::unique_lock<std::mutex> lock(qps_mutex);
        //auto it = qps.find(t);
        //if (it != qps.end()) {
        //    ++(it->second);
        //} else {
        //    qps[t] = 1;
        //}

        send(req);
    }
}

int user_init() {
    socket_server.Init(&config, &status);
    socket_server.set_recv_done(deal);

    socket_server.Start();
    return 0;
}

void user_destroy() { socket_server.Stop(); }

int main(int argc, char *argv[]) {
    sf_regist_print_version_callback(print_version);

    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);

    sf_regist_load_config_callback(load_conf_file);

    sf_regist_user_init_callback(user_init);
    sf_regist_user_destroy_callback(user_destroy);

    sf_service_run(argc, argv, "test_server");

    for (auto &q : qps) {
        std::cout << "time: " << q.first << " Qps: " << q.second << std::endl;
    }

    return 0;
}
