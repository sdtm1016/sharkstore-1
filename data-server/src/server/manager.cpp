#include "manager.h"

#include <assert.h>
#include <chrono>
#include <string>

#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_config.h"
#include "frame/sf_logger.h"
#include "frame/sf_socket_buff.h"
#include "frame/sf_status.h"
#include "frame/sf_util.h"

#include "callback.h"
#include "run_status.h"
#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

int Manager::Init(ContextServer *context) {
    FLOG_INFO("Manager Init begin ...");

    strcpy(ds_config.manager_config.thread_name_prefix, "manage");

    if (socket_server_.Init(&ds_config.manager_config, &manager_status_) != 0) {
        FLOG_INFO("Manager Init error ...");
        return -1;
    }

    socket_server_.set_recv_done(ds_manager_deal_callback);
    socket_server_.set_send_done(ds_send_done_callback);

    context_ = context;
    g_status.manager_socket_status = &manager_status_;

    FLOG_INFO("Manager Init end ...");
    return 0;
}

int Manager::Start() {
    FLOG_INFO("Manager Start begin ...");

    char name[16];
    for (int i = 0; i < ds_config.manager_config.worker_threads; i++) {
        worker_.emplace_back([&] {
            common::ProtoMessage *task;
            while (g_continue_flag) {
                task = NULL;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    if (queue_.empty()) {
                        cond_.wait_for(lock, std::chrono::seconds(5));
                        continue;
                    }

                    task = queue_.front();
                    queue_.pop();
                }

                DealTask(task);
            }

            FLOG_INFO("Manager thread exit...");
            __sync_fetch_and_sub(&manager_status_.actual_worker_threads, 1);
        });
        __sync_fetch_and_add(&manager_status_.actual_worker_threads, 1);

        auto handle = worker_[i].native_handle();
        sprintf(name, "manager:%d", i);
        AnnotateThread(handle, name);
    }

    if (socket_server_.Start() != 0) {
        FLOG_ERROR("Manager Start error ...");
        return -1;
    };

    FLOG_INFO("Manager Start end ...");
    return 0;
}

void Manager::Stop() {
    FLOG_INFO("Manager Stop begin ...");

    socket_server_.Stop();

    cond_.notify_all();
    {
        std::unique_lock<std::mutex> lock(mutex_);

        common::ProtoMessage *task;

        while (!queue_.empty()) {
            task = queue_.front();
            queue_.pop();
            delete task;
        }
    }

    auto size = worker_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (worker_[i].joinable()) {
            worker_[i].join();
        }
    }

    FLOG_INFO("Manager Stop end ...");
}

void Manager::Push(common::ProtoMessage *task) {
    task->socket = &socket_server_;

    if (task->header.func_id == 0) {  // funcpb::FunctionID::kFuncHeartbeat = 0
        FLOG_WARN("Heartbeat session_id %" PRIu64, task->session_id);
        context_->socket_session->Send(task, nullptr);
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push(task);
    cond_.notify_one();
}

void Manager::DealTask(common::ProtoMessage *task) {
    if (task->expire_time < getticks()) {
        FLOG_WARN("msg_id %" PRIu64 " is expired ", task->header.msg_id);
        delete task;
        return;
    }

    DataServer::Instance().DealTask(task);
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
