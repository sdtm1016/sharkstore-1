#ifndef __SOCKET_CLINET_H__
#define __SOCKET_CLINET_H__

#include <unordered_map>
#include <vector>
#include <queue>
#include <chrono>
#include <thread>
#include <mutex>
#include <tuple>
#include <condition_variable>
#include <memory>

#include "socket_session.h"
#include "socket_base.h"

namespace sharkstore {
namespace dataserver {
namespace common {

static const int k_slot = 16;

class SocketClient : public SocketBase {
public:
    explicit SocketClient(SocketSession *ss);
    ~SocketClient() = default;

    SocketClient(const SocketClient &) = delete;
    SocketClient& operator=(const SocketClient &) = delete;
    SocketClient& operator=(const SocketClient &) volatile = delete;

    virtual int Init(sf_socket_thread_config_t *config, sf_socket_status_t *status);
    virtual int Start();
    virtual void Stop();

    ProtoMessage *GetRequest(const void *data) {
        return GetProtoMessage(data);
    }

    //timeout:ms
    int SyncSend(int64_t msg_id, response_buff_t *buff, int timeout=10000);

    std::tuple<int64_t, bool> get_session_id(const char*ip_addr, const int port);

    //if ok return ProtoMessage pointer, else return nullptr
    ProtoMessage *SyncRecv(int64_t msg_id, int timeout = 10000);

    //timeout:ms
    int AsyncSend(int64_t msg_id, response_buff_t *buff, int timeout=10000);

    ProtoMessage *CompletionQueuePop();

    int RecvDone(ProtoMessage *req);

    void CheckTimeout();

private:
    enum {SyncIo, AsyncIo};

    struct RecvNotify {
        int64_t msg_id;
        char msg_id_type;

        ProtoMessage *result;
        std::mutex notify_mutex;
        std::condition_variable notify_cond;
    };

    typedef std::pair<time_t, int64_t> tr;

    struct MsgMap {
        //The object is deleted when the timeout occurs or it is received
        std::unordered_map<int64_t, std::shared_ptr<RecvNotify>> result;
        std::priority_queue<tr, std::vector<tr>, std::greater<tr>> timeout;

        std::mutex msg_mutex;
        std::condition_variable msg_cond;
    };

    struct SessionMap {
        std::mutex map_mutex;
        std::unordered_map<int64_t, std::vector<int64_t>> ids;
    };

    struct CompletionQueue {
        std::mutex com_mutex;
        std::queue<ProtoMessage*> com_queue;
    };

    SocketSession * socket_session_;

    MsgMap msg_map_[k_slot];
    SessionMap session_map_[k_slot];
    CompletionQueue  completion_queue_[k_slot];

    std::thread check_timeout_thread_;

    int defualt_timeout_ = 10 * 1000;//10s
    int max_conn_per_session_ = 2;
};

}//namespace sharkstore;
}//namespace sharkstore;
}//namespace sharkstore;
#endif//__SOCKET_CLINET_H__
