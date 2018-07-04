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

#include <fastcommon/logger.h>

#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "common/ds_types.h"
#include "common/ds_version.h"
#include "common/socket_session_impl.h"
#include "frame/sf_service.h"
#include "frame/sf_socket_buff.h"
#include "frame/sf_status.h"
#include "frame/sf_util.h"
#include "server/callback.h"
#include "server/manager.h"
#include "server/worker.h"

sf_socket_status_t manager_status = {0};
sf_socket_status_t worker_status = {0};

using namespace sharkstore::dataserver::common;

SocketServer ws_server;
SocketServer ms_server;
SocketSession *socket_session = new SocketSessionImpl;

int deal_manager(request_buff_t *request, void *args) {
    ProtoMessage *req = socket_session->GetProtoMessage(request->buff);

    req->session_id = request->session_id;
    req->expire_time = getticks();

    if (req->header.time_out > 0) {
        req->expire_time += req->header.time_out;
    } else {
        req->expire_time += 100000;
    }

    ds_proto_header_t *proto_header = (ds_proto_header_t *)request->buff;
    // 解析头部
    int len = sizeof(ds_proto_header_t) + req->header.body_len;

    response_buff_t *send_task = new_response_buff(len + 100);

    memcpy(send_task->buff, request->buff, len);
    strcpy(send_task->buff + len, " manager call\n");

    req->header.body_len += strlen(" manager call\n");

    send_task->expire_time = getticks() + 1000000;
    send_task->session_id = req->session_id;
    send_task->buff_len = request->buff_len;
    send_task->buff_len += strlen(" manager call\n");

    ds_serialize_header(&req->header, proto_header);

    memcpy(send_task->buff, (void *)proto_header, sizeof(ds_proto_header_t));

    // send response
    ms_server.Send(send_task);
    delete req;
    return 0;
}

int deal_worker(request_buff_t *request, void *args) {
    ProtoMessage *req = socket_session->GetProtoMessage(request->buff);

    req->session_id = request->session_id;
    req->expire_time = getticks();

    if (req->header.time_out > 0) {
        req->expire_time += req->header.time_out;
    } else {
        req->expire_time += 100000;
    }

    int len = sizeof(ds_proto_header_t) + req->header.body_len;

    response_buff_t *send_task = new_response_buff(len + 100);

    // test delete_response_buff
    if (true) {
        delete_response_buff(send_task);
    }

    send_task = new_response_buff(len + 100);

    req->header.msg_type = 0x12;
    ds_serialize_header(&req->header, (ds_proto_header_t *)send_task->buff);

    memcpy(send_task->buff, request->buff, len);
    strcpy(send_task->buff + len, " worker call\n");

    send_task->expire_time = req->header.time_out + getticks();
    send_task->session_id = req->session_id;
    send_task->buff_len = request->buff_len;
    send_task->buff_len += strlen(" worker call\n");

    ws_server.Send(send_task);

    delete req;
    return 0;
}

int user_init() {
    ms_server.Init(&ds_config.manager_config, &manager_status);
    ws_server.Init(&ds_config.worker_config, &worker_status);

    ms_server.Start();
    ws_server.Start();
    return 0;
}

void user_destroy() {
    ms_server.Stop();
    ws_server.Stop();
}

int main(int argc, char *argv[]) {
    sf_regist_print_version_callback(print_version);

    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);

    sf_regist_load_config_callback(load_from_conf_file);

    sf_regist_user_init_callback(user_init);
    sf_regist_user_destroy_callback(user_destroy);

    sf_service_run(argc, argv, "test_server");

    return 0;
}
