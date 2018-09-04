#ifndef __WORKER_H__
#define __WORKER_H__

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "common/ds_config.h"
#include "common/socket_server.h"
#include "frame/sf_status.h"
#include "lk_queue/blockingconcurrentqueue.h"

#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class Worker final {
public:
    Worker() : slot_seed_(0) {};
    ~Worker() = default;

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;
    Worker &operator=(const Worker &) volatile = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();

    // 0: fast queue; 1: slow queue; 2: thread queue
    void Push(common::ProtoMessage *task);

    void PrintQueueSize();

    size_t ClearQueue(bool fast, bool slow);

    uint64_t FastQueueSize() const { return fast_queue_.all_msg_size; }
    uint64_t SlowQueueSize() const { return slow_queue_.all_msg_size; }

    // TODO:
    void GetPending() const {}

private:

    struct MsgQueue {
        moodycamel::BlockingConcurrentQueue<common::ProtoMessage *> msg_queue;
    };

    struct HashQueue {
        std::vector<MsgQueue *> msg_queue;
        std::atomic<uint64_t> all_msg_size;

        HashQueue() : all_msg_size(0) {}
    };

    bool isSlow(common::ProtoMessage *msg);

    void DealTask(common::ProtoMessage *task);
    void Clean(HashQueue &hash_queue);

    void StartWorker(std::vector<std::thread> &worker, HashQueue & hash_queue, int num);

private:
    std::atomic<uint64_t> slot_seed_;
    std::vector<std::thread> fast_worker_;
    std::vector<std::thread> slow_worker_;

    HashQueue fast_queue_;
    HashQueue slow_queue_;

    common::SocketServer socket_server_;

    sf_socket_status_t worker_status_ = {0};

    ContextServer *context_ = nullptr;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

#endif  //__WORKER_H__
