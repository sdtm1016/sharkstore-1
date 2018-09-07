#include <unistd.h>
#include <iostream>

#include "bench.h"

#include "frame/sf_service.h"
#include "frame/sf_status.h"
#include "common/ds_proto.h"
#include "common/socket_session_impl.h"
#include "common/socket_server.h"
#include "proto/gen/kvrpcpb.pb.h"

using namespace sharkstore::dataserver::common;

sf_socket_thread_config_t config;
sf_socket_status_t status = {0};

SocketSession *socket_session = new SocketSessionImpl;
SocketServer socket_server;

static void print_version() {
    fprintf(stderr, "ds version 1.2.3\n");
}

static int load_conf_file(IniContext *ini_context, const char *filename) {
    return sf_load_socket_thread_config(ini_context, "worker", &config);
}

static void deal(request_buff_t *request, void *args) {
    auto proto_header = (ds_proto_header_t *)(request->buff);
    ds_header_t req_header;
    ds_unserialize_header(proto_header, &req_header);
    if (req_header.func_id != kBenchFuncID) {
        std::cerr << "invalid funcid " << req_header.func_id << std::endl;
        exit(EXIT_FAILURE);
    }

    auto resp = new kvrpcpb::DsKvRawPutResponse;

    size_t body_len = resp->ByteSizeLong();
    size_t data_len = header_size + body_len;
    response_buff_t *response = new_response_buff(data_len);

    // 填充应答头部
    ds_header_t resp_header;
    resp_header.magic_number = DS_PROTO_MAGIC_NUMBER;
    resp_header.body_len = static_cast<int>(body_len);
    resp_header.msg_id = req_header.msg_id;
    resp_header.version = DS_PROTO_VERSION_CURRENT;
    resp_header.msg_type = DS_PROTO_FID_RPC_RESP;
    resp_header.func_id = req_header.func_id;
    resp_header.proto_type = req_header.proto_type;
    ds_serialize_header(&resp_header, (ds_proto_header_t *)(response->buff));

    response->session_id  = request->session_id;
    response->msg_id      = req_header.msg_id;
    response->begin_time  = request->begin_time;
    response->expire_time = 5000;
    response->buff_len    = static_cast<int32_t>(data_len);

    char *data = response->buff + header_size;
    resp->SerializeToArray(data, body_len);

    socket_server.Send(response);
}

int user_init() {
    std::cout << "Recv Threads: " << config.event_recv_threads << std::endl;

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
}
