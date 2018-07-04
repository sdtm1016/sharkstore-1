#include "socket_client.h"

#include "frame/sf_util.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace common {

static void client_recv_done(request_buff_t *request, void *args) {
    SocketClient *sc = static_cast<SocketClient *>(args);
    ProtoMessage *req = sc->GetRequest(request->buff);

    if (req != nullptr) {
        req->session_id = request->session_id;
        req->expire_time = getticks();
        sc->RecvDone(req);

        FLOG_DEBUG("recv done, msg_id %" PRId64 , req->header.msg_id);
    }
}

SocketClient::SocketClient(SocketSession *ss) : socket_session_(ss) {}

int SocketClient::Init(sf_socket_thread_config_t *config, sf_socket_status_t *status) {
    SocketBase::Init(config, status);

    thread_info_.recv_callback = client_recv_done;
    thread_info_.user_data = this;
    thread_info_.is_client = true;

    return 0;
}

int SocketClient::Start() {
    if (SocketBase::Start() == 0) {
        check_timeout_thread_ = std::thread(&SocketClient::CheckTimeout, this);
        return 0;
    }
    return -1;
}

void SocketClient::Stop() {
    for (auto &msg_map : msg_map_) {
        msg_map.msg_cond.notify_all();
    }

    if (check_timeout_thread_.joinable()) {
        check_timeout_thread_.join();
    }

    SocketBase::Stop();

    for (auto &msg_map : msg_map_) {
        std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
        msg_map.result.clear();
    }

    for (auto &msg_queue : completion_queue_) {
        std::lock_guard<std::mutex> lock(msg_queue.com_mutex);
        if (!msg_queue.com_queue.empty()) {
            ProtoMessage *req = msg_queue.com_queue.front();
            msg_queue.com_queue.pop();
            delete req;
        }
    }
}


std::tuple<int64_t, bool> SocketClient::get_session_id(const char *ip_addr,
                                                       const int port) {
    int64_t session_id = sf_get_session(ip_addr, port);

    auto &session = session_map_[session_id & (k_slot - 1)];
    std::lock_guard<std::mutex> lock(session.map_mutex);

    auto it = session.ids.find(session_id);
    if (it == session.ids.end()) {
        it = session.ids.emplace(session_id, std::vector<int64_t>()).first;
    }

    auto &ids = it->second;
    int try_times = 3;

    while (try_times--) {
        auto plot = getticks() % max_conn_per_session_;

        if (plot >= (int)ids.size()) {
            // Wait for optimization
            // No lock should be made when creating a connection
            auto id = sf_connect_session_get(&thread_info_, ip_addr, port);
            if (id > 0) {
                ids.push_back(id);
                return std::make_tuple(id, true);
            }
            continue;
        }

        auto id = ids[plot];
        if (!Closed(id)) {
            return std::make_tuple(id, false);
        }

        ids.erase(ids.begin() + plot);
    }

    return std::make_tuple(-1, false);
}

int SocketClient::SyncSend(int64_t msg_id, response_buff_t *buff, int timeout) {
    std::shared_ptr<RecvNotify> notify = std::make_shared<RecvNotify>();

    notify->msg_id = msg_id;
    notify->msg_id_type = SyncIo;
    notify->result = nullptr;

    auto &msg_map = msg_map_[msg_id & (k_slot - 1)];

    std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
    msg_map.result[msg_id] = notify;

    if (timeout <= 0) {
        msg_map.timeout.emplace(getticks() + defualt_timeout_, msg_id);
    } else {
        msg_map.timeout.emplace(getticks() + timeout, msg_id);
    }

    return Send(buff);
}

ProtoMessage *SocketClient::SyncRecv(int64_t msg_id, int timeout) {
    std::shared_ptr<RecvNotify> notify;

    auto &msg_map = msg_map_[msg_id & (k_slot - 1)];

    do {
        std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
        auto it = msg_map.result.find(msg_id);
        if (it == msg_map.result.end()) {  // expired
            return nullptr;
        }

        notify = it->second;
        if (notify->result != nullptr) {
            auto req = notify->result;
            msg_map.result.erase(it);

            return req;
        }
    } while (false);

    if (timeout <= 0) {
        timeout = defualt_timeout_;
    }

    // recv or timeout notify
    std::unique_lock<std::mutex> notify_lock(notify->notify_mutex);

    if (notify->result == nullptr) {
        notify->notify_cond.wait_for(notify_lock, std::chrono::milliseconds(timeout));
    }

    std::lock_guard<std::mutex> lock(msg_map.msg_mutex);

    msg_map.result.erase(msg_id);
    if (notify->result != nullptr) {
        auto req = notify->result;
        return req;
    }

    return nullptr;
}

int SocketClient::AsyncSend(int64_t msg_id, response_buff_t *buff, int timeout) {
    std::shared_ptr<RecvNotify> notify = std::make_shared<RecvNotify>();

    notify->msg_id = msg_id;
    notify->msg_id_type = AsyncIo;
    notify->result = nullptr;

    auto &msg_map = msg_map_[msg_id & (k_slot - 1)];

    do {
        std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
        msg_map.result[msg_id] = notify;

        if (timeout <= 0) {
            msg_map.timeout.emplace(getticks() + defualt_timeout_, msg_id);
        } else {
            msg_map.timeout.emplace(getticks() + timeout, msg_id);
        }
    } while (false);

    return Send(buff);
}

ProtoMessage *SocketClient::CompletionQueuePop() {

    auto slot = getticks() & (k_slot -1);
    auto e_slot = slot + k_slot;
    while (g_continue_flag && slot < e_slot) {
        auto &msg_queue = completion_queue_[(slot++) & (k_slot -1)];
        if (msg_queue.com_queue.empty()) {
            continue;
        }

        std::lock_guard<std::mutex> lock(msg_queue.com_mutex);

        if (!msg_queue.com_queue.empty()) {
            ProtoMessage *req = msg_queue.com_queue.front();
            msg_queue.com_queue.pop();
            return req;
        }
    }

    return nullptr;
}

void SocketClient::CheckTimeout() {
    uint64_t p_slot = 0;
    int64_t min_time = 100;//100ms

    while (g_continue_flag) {
        auto slot = (p_slot++) & (k_slot -1);
        if (slot == 0) {
            if (min_time > 0) {
                usleep(min_time * 1000);
            }
            min_time = 100;
        }

        auto &msg_map = msg_map_[slot];

        std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
        if (msg_map.timeout.empty()) {
            continue;
        }

        auto cur_time = getticks();
        auto pr = msg_map.timeout.top();
        if (pr.first > cur_time) {
            auto it = msg_map.result.find(pr.second);
            if (it != msg_map.result.end()) {
                if (it->second->msg_id_type == SyncIo) {
                    // already recv,clean this notify
                    msg_map.timeout.pop();
                }
            } else {
                auto mt = pr.first - cur_time;
                if (min_time > mt) {
                    min_time = mt;
                }
            }
            continue;
        }

        msg_map.timeout.pop();

        // expired
        auto it = msg_map.result.find(pr.second);
        if (it != msg_map.result.end()) {
            if (it->second->msg_id_type == SyncIo) {
                if (it->second->result != nullptr) {
                    delete it->second->result;
                    it->second->result = nullptr;
                }
                it->second->notify_cond.notify_one();
            } else {
                ProtoMessage *req = new ProtoMessage;
                req->msg_id = pr.second;
                req->expire_time = 0;  // set expired

                msg_map.result.erase(it);

                auto &msg_queue = completion_queue_[req->msg_id & (k_slot -1)];
                std::lock_guard<std::mutex> lock(msg_queue.com_mutex);
                msg_queue.com_queue.push(req);
            }
        }
    }
}

int SocketClient::RecvDone(ProtoMessage *req) {
    do {

        std::shared_ptr<RecvNotify> notify;

        auto slot = req->header.msg_id & (k_slot - 1);
        auto &msg_map = msg_map_[slot];
        {

            std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
            auto it = msg_map.result.find(req->header.msg_id);
            // not found as deleted expired msg_id
            if (it == msg_map.result.end()) {
                FLOG_DEBUG("recv timeout, msg_id %" PRId64 , req->header.msg_id);
                break;
            }

            notify = it->second;
        }

        if (req->header.time_out > 0) {
            req->expire_time += req->header.time_out;
        } else {
            req->expire_time += defualt_timeout_;  // 10s
        }

        if (notify->msg_id_type == SyncIo) {
            std::unique_lock<std::mutex> notify_lock(notify->notify_mutex);
            notify->result = req;
            notify->notify_cond.notify_one();
        } else {  // async
            std::lock_guard<std::mutex> lock(msg_map.msg_mutex);
            //msg_map.result.erase(it);

            auto &msg_queue = completion_queue_[slot];
            std::lock_guard<std::mutex> qlock(msg_queue.com_mutex);
            msg_queue.com_queue.push(req);
        }

        return 0;
    } while (false);

    delete req;
    return -1;
}

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
