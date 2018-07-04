#include "callback.h"

#include "frame/sf_logger.h"
#include "frame/sf_util.h"

#include "manager.h"
#include "monitor/syscommon.h"
#include "run_status.h"
#include "server.h"
#include "worker.h"

using sharkstore::dataserver::common::ProtoMessage;
using sharkstore::dataserver::server::DataServer;

extern "C" {

void ds_manager_deal_callback(request_buff_t *request, void *args) {
    auto cs = DataServer::Instance().context_server();
    ProtoMessage *req = cs->socket_session->GetProtoMessage(request->buff);
    if (req != nullptr) {
        req->session_id = request->session_id;
        req->begin_time = get_micro_second();
        req->expire_time = getticks();

        if (req->header.time_out > 0) {
            req->expire_time += req->header.time_out;
        } else {
            req->expire_time += ds_config.task_timeout;
        }

        FLOG_INFO("new proto message. session_id: %" PRId64 ",msgid: %" PRId64
                  ", func_id: %d, body_len: %d",
                  request->session_id, req->header.msg_id, req->header.func_id,
                  req->header.body_len);
        cs->manager->Push(req);
    }
}

void ds_worker_deal_callback(request_buff_t *request, void *args) {
    auto cs = DataServer::Instance().context_server();
    ProtoMessage *req = cs->socket_session->GetProtoMessage(request->buff);

    if (req != nullptr) {
        req->session_id = request->session_id;
        req->begin_time = get_micro_second();
        req->expire_time = getticks();

        if (req->header.time_out > 0) {
            req->expire_time += req->header.time_out;
        } else {
            req->expire_time += ds_config.task_timeout;
        }

        FLOG_DEBUG("new proto message. session_id: %" PRId64 ",msgid: %" PRId64
                   ", func_id: %d, body_len: %d",
                   request->session_id, req->header.msg_id, req->header.func_id,
                   req->header.body_len);

        cs->worker->Push(req);
    }
}

void ds_send_done_callback(response_buff_t *response, void *args, int err) {
    uint32_t take_time = get_micro_second() - response->begin_time;

    FLOG_DEBUG("session_id: %" PRId64 ",task msgid: %" PRId64
               " execute take time: %d us",
               response->session_id, response->msg_id, take_time);

    auto cs = DataServer::Instance().context_server();
    cs->run_status->PushTime(sharkstore::monitor::PrintTag::Deal, take_time);
}

int ds_user_init_callback() { return DataServer::Instance().Start(); }

void ds_user_destroy_callback() { DataServer::Instance().Stop(); }

}  // extern C
