#include "worker.h"

#include <assert.h>
#include <chrono>

#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "frame/sf_config.h"
#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "proto/gen/funcpb.pb.h"

#include "callback.h"
#include "run_status.h"
#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

int Worker::Init(ContextServer *context) {
    FLOG_INFO("Worker Init begin ...");

    strcpy(ds_config.worker_config.thread_name_prefix, "work");

    if (socket_server_.Init(&ds_config.worker_config, &worker_status_) != 0) {
        FLOG_ERROR("Worker Init error ...");
        return -1;
    }

    socket_server_.set_recv_done(ds_worker_deal_callback);
    socket_server_.set_send_done(ds_send_done_callback);

    context_ = context;
    g_status.worker_socket_status = &worker_status_;

    FLOG_INFO("Worker Init end ...");
    return 0;
}

void Worker::StartWorker(std::vector<std::thread> &worker,
                         HashQueue &hash_queue, int num) {
    hash_queue.msg_queue.resize(num);

    for (int i = 0; i < num; i++) {
        auto mq = new MsgQueue;
        hash_queue.msg_queue[i] = mq;

        worker.emplace_back([&, mq] {
            int batch_count = 10;
            common::ProtoMessage *task;

            while (g_continue_flag) {
                task = nullptr;
                if (mq->msg_queue.wait_dequeue_timed(
                        task, std::chrono::milliseconds(100))) {
                    if (!g_continue_flag) {
                        delete task;
                        break;
                    }

                    if (task != nullptr) {
                        --hash_queue.all_msg_size;
                        DealTask(task);
                    }
                }
            }

            FLOG_INFO("Worker thread exit...");
            __sync_fetch_and_sub(&worker_status_.actual_worker_threads, 1);

        });

        __sync_fetch_and_add(&worker_status_.actual_worker_threads, 1);
    }
}

int Worker::Start() {
    FLOG_INFO("Worker Start begin ...");

    // start fast worker
    StartWorker(fast_worker_, fast_queue_, ds_config.fast_worker_num);

    int i = 0;
    char fast_name[16];
    for (auto &work : fast_worker_) {
        auto handle = work.native_handle();
        sprintf(fast_name, "fast_worker:%d", i++);
        AnnotateThread(handle, fast_name);
    }
    // start slow worker
    StartWorker(slow_worker_, slow_queue_, ds_config.slow_worker_num);

    char slow_name[16];
    i = 0;
    for (auto &work : slow_worker_) {
        auto handle = work.native_handle();
        sprintf(slow_name, "slow_worker:%d", i++);
        AnnotateThread(handle, slow_name);
    }

    if (socket_server_.Start() != 0) {
        FLOG_ERROR("Worker Start error ...");
        return -1;
    }

    FLOG_INFO("Worker Start end ...");
    return 0;
}

void Worker::Stop() {
    FLOG_INFO("Worker Stop begin ...");

    socket_server_.Stop();

    auto size = fast_worker_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (fast_worker_[i].joinable()) {
            fast_worker_[i].join();
        }
    }

    size = slow_worker_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (slow_worker_[i].joinable()) {
            slow_worker_[i].join();
        }
    }

    Clean(fast_queue_);
    Clean(slow_queue_);

    FLOG_INFO("Worker Stop end ...");
}

void Worker::Push(common::ProtoMessage *task) {
    sf_session_entry_t *entry;
    task->socket = &socket_server_;

    if (task->header.func_id == 0) {  // funcpb::FunctionID::kFuncHeartbeat = 0
        entry = task->socket->lookup_session_entry(task->session_id);
        if (entry == nullptr) {
            FLOG_DEBUG("Heartbeat ip: %s, session_id %" PRIu64,
                       entry->rtask->client_ip, task->session_id);
        } else {
            FLOG_DEBUG("session: %" PRId64 " is alive", task->session_id);
        }

        context_->socket_session->Send(task, nullptr);
        return;
    }

    int type = FuncType(task);

    if (type == 0) {
        auto slot = ++slot_seed_ % ds_config.fast_worker_num;
        auto mq = fast_queue_.msg_queue[slot];

        mq->msg_queue.enqueue(task);

        ++fast_queue_.all_msg_size;

    } else if (type == 1) {
        auto slot = ++slot_seed_ % ds_config.slow_worker_num;
        auto mq = slow_queue_.msg_queue[slot];

        mq->msg_queue.enqueue(task);

        ++slow_queue_.all_msg_size;
    }
}

void Worker::DealTask(common::ProtoMessage *task) {
    if (task->expire_time < getticks()) {
        FLOG_ERROR("msg_id %" PRIu64 " is expired ", task->header.msg_id);
        delete task;
        return;
    }

    DataServer::Instance().DealTask(task);
}

void Worker::Clean(HashQueue &hash_queue) {
    for (auto mq : hash_queue.msg_queue) {
        common::ProtoMessage *task;
        while (mq->msg_queue.try_dequeue(task)) {
            delete task;
        }
        delete mq;
    }
}

// 0: fast queue; 1: slow queue;
int Worker::FuncType(common::ProtoMessage *msg) {
    if (msg->header.func_id == funcpb::FunctionID::kFuncSelect) {
        return 1;
    }

    return 0;
}

void Worker::PrintQueueSize() {
    FLOG_INFO("worker fast queue size:%" PRIu64,
              fast_queue_.all_msg_size.load());
    FLOG_INFO("worker slow queue size:%" PRIu64,
              slow_queue_.all_msg_size.load());
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
