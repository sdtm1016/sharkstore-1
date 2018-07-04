#ifndef __DS_MANAGER_H__
#define __DS_MANAGER_H__

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "common/socket_server.h"
#include "frame/sf_status.h"

#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class Manager final {
public:
    Manager() = default;
    ~Manager() = default;

    Manager(const Manager &) = delete;
    Manager &operator=(const Manager &) = delete;
    Manager &operator=(const Manager &) volatile = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();

    void Push(common::ProtoMessage *task);

private:
    void DealTask(common::ProtoMessage *task);

private:
    std::queue<common::ProtoMessage *> queue_;

    std::mutex mutex_;
    std::condition_variable cond_;

    std::vector<std::thread> worker_;

    common::SocketServer socket_server_;

    sf_socket_status_t manager_status_ = {0};

    ContextServer *context_ = nullptr;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore

#endif  //__DS_MANAGER_H__
