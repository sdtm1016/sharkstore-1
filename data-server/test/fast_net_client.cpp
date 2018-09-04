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
#include <map>
#include <atomic>
#include <mutex>
#include <thread>

#include "common/ds_proto.h"
#include "common/ds_types.h"
#include "common/ds_version.h"
#include "common/socket_client.h"
#include "common/socket_session_impl.h"
#include "frame/sf_service.h"
#include "frame/sf_socket_buff.h"
#include "frame/sf_status.h"
#include "frame/sf_util.h"
#include "frame/sf_logger.h"

#include "helper/gen/test.pb.h"

sf_socket_thread_config_t config;
sf_socket_status_t status = {0};

using namespace sharkstore::dataserver::common;

SocketSession *socket_session = new SocketSessionImpl;

SocketClient socket_client(socket_session);

std::atomic<int64_t> msg_id(1);
int send_thread = 10;
int send_times = 100000;

size_t proto_len = sizeof(ds_proto_header_t);

std::map<int64_t, uint64_t> qps;
std::mutex qps_mutex;

static int load_conf_file(IniContext *ini_context, const char *filename) {
    return sf_load_socket_thread_config(ini_context, "client", &config);
}

static int64_t get_proto_head(ds_proto_header_t *proto_header, int len) {
    ds_header_t header;
    auto id = msg_id++;

    header.magic_number = DS_PROTO_MAGIC_NUMBER;
    header.version = 100;
    header.body_len = len;
    header.time_out = 10000;
    header.msg_id = id;
    header.msg_type = 10;
    header.func_id = 10;
    header.proto_type = 1;
    header.flags = 0;

    ds_serialize_header(&header, proto_header);
    return id;
}

static void client_send() {
    int times = send_times;
    auto b = get_micro_second();
    while (g_continue_flag && times--) {
        tpb::TestMsg *msg = new tpb::TestMsg;
        msg->set_message("this is fast net test !!!");

        bool is_first;

        int64_t session_id;
        std::tie(session_id, is_first) =
            socket_client.get_session_id(config.ip_addr, config.port);

        size_t body_len = msg == nullptr ? 0 : msg->ByteSizeLong();

        response_buff_t *resp = new_response_buff(proto_len + body_len);

        ds_proto_header_t *proto = (ds_proto_header_t *)(resp->buff);

        auto id = get_proto_head(proto, body_len);
        msg->SerializeToArray(resp->buff + proto_len, body_len);

        delete msg;

        resp->session_id = session_id;
        resp->buff_len = proto_len + body_len;

        auto bs = get_micro_second();
        //socket_client.AsyncSend(id, resp);
        FLOG_INFO("client send msg_id:%" PRIu64, id);
        socket_client.SyncSend(id, resp);
        auto ret = socket_client.SyncRecv(id);

        auto es = get_micro_second();
        FLOG_INFO("client recv done msg_id:%" PRIu64, id);
        if (es - bs > 1000) {
            FLOG_INFO("client msg_id:%" PRIu64 " take time %" PRIu64, id, es-bs);
        }
        if (ret != nullptr) {
            auto t = time(NULL);
            std::unique_lock<std::mutex> lock(qps_mutex);
            auto it = qps.find(t);
            if (it != qps.end()) {
                ++(it->second);
            } else {
                qps[t] = 1;
            }

            delete ret;
        }
    }

    auto e = get_micro_second();
    FLOG_INFO("send %d take time %f ms", send_times, (e-b) * 0.001);
}

static void client_recv() {

    int recv_times = 0;
    auto b = get_micro_second();
    while (g_continue_flag && recv_times++ < send_times) {
        while (g_continue_flag) {
            ProtoMessage *msg = socket_client.CompletionQueuePop();
            if (msg != nullptr) {
                if (msg->body.size() > 0) {
                    tpb::TestMsg tmsg;
                    GetMessage(msg->body.data(), msg->body.size(), &tmsg);
                }
                delete msg;
                auto t = time(NULL);
                std::unique_lock<std::mutex> lock(qps_mutex);
                auto it = qps.find(t);
                if (it != qps.end()) {
                    ++(it->second);
                } else {
                    qps[t] = 1;
                }
                break;
            }
        }
    }

    auto e = get_micro_second();
    FLOG_ERROR("recv %d take time %f ms", send_times, (e-b) * 0.001);
}

int user_init() {
    socket_client.Init(&config, &status);
    socket_client.Start();

    std::vector<std::thread> send_test;
    for (int i=0; i<send_thread; i++) {
        send_test.emplace_back(client_send);
    }

    //std::vector<std::thread> recv_test;
    //for (int i=0; i<send_thread; i++) {
    //    recv_test.emplace_back(client_recv);
    //}

    for (auto &t : send_test) {
        t.join();
    }

    //for (auto &t : recv_test) {
    //    t.join();
    //}

    for (auto &q : qps) {
        FLOG_ERROR("time: %" PRId64 " Qps: %" PRIu64, q.first, q.second);
    }

    return 0;
}

void user_destroy() { socket_client.Stop(); }

int main(int argc, char *argv[]) {
    if (argc > 4) {
        send_times = atoi(argv[4]);
    }
    if (argc > 5) {
        send_thread = atoi(argv[5]);
    }

    std::cout << "send times:" << send_times;

    sf_regist_print_version_callback(print_version);

    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);

    sf_regist_load_config_callback(load_conf_file);

    sf_regist_user_init_callback(user_init);
    sf_regist_user_destroy_callback(user_destroy);

    sf_service_run(argc, argv, "fast_client");

    return 0;
}
