#include "server_impl.h"

#include <thread>

#include "logger.h"
#include "raft_exception.h"
#include "raft_impl.h"
#include "snapshot/manager.h"
#include "transport/fast_transport.h"
#include "transport/inprocess_transport.h"
#include "transport/transport.h"

namespace sharkstore {
namespace raft {
namespace impl {

RaftServerImpl::RaftServerImpl(const RaftServerOptions& ops) : ops_(ops) {
    tick_msg_.reset(new pb::Message);
    tick_msg_->set_type(pb::LOCAL_MSG_TICK);
}

RaftServerImpl::~RaftServerImpl() {
    Stop();

    for (auto t: consensus_threads_) {
        delete t;
    }
    for (auto t : apply_threads_) {
        delete t;
    }
}

Status RaftServerImpl::Start() {
    auto status = ops_.Validate();
    if (!status.ok()) {
        return status;
    }

    // 初始化raft工作线程池
    for (int i = 0; i < ops_.consensus_threads_num; ++i) {
        auto t = new WorkThread(this, ops_.consensus_queue_capacity,
                                std::string("raft-worker:") + std::to_string(i));
        consensus_threads_.push_back(t);
    }
    LOG_INFO("raft[server] %d consensus threads start. queue capacity=%d",
             ops_.consensus_threads_num, ops_.consensus_queue_capacity);

    // 初始化apply工作线程池
    for (int i = 0; i < ops_.apply_threads_num; ++i) {
        auto t = new WorkThread(this, ops_.apply_queue_capacity,
                                std::string("raft-apply:") + std::to_string(i));
        apply_threads_.push_back(t);
    }
    LOG_INFO("raft[server] %d apply threads start. queue capacity=%d",
             ops_.apply_threads_num, ops_.apply_queue_capacity);

    // start transport
    if (ops_.transport_options.use_inprocess_transport) {
        transport_.reset(new transport::InProcessTransport(ops_.node_id));
    } else {
        transport_.reset(new transport::FastTransport(ops_.transport_options.resolver,
                                                  ops_.transport_options.send_io_threads,
                                                  ops_.transport_options.recv_io_threads));
    }
    status = transport_->Start(
        ops_.transport_options.listen_ip, ops_.transport_options.listen_port,
        std::bind(&RaftServerImpl::onMessage, this, std::placeholders::_1));
    if (!status.ok()) {
        return status;
    }

    // start snapshot sender
    assert(snapshot_manager_ == nullptr);
    snapshot_manager_.reset(new SnapshotManager(ops_.snapshot_options));

    running_ = true;
    tick_thr_.reset(new std::thread([this]() {
        tickRoutine(); }));

    return Status::OK();
}

Status RaftServerImpl::Stop() {
    if (!running_) return Status::OK();

    running_ = false;

    if (tick_thr_ && tick_thr_->joinable()) tick_thr_->join();

    for (auto& t : consensus_threads_) {
        t->shutdown();
    }

    for (auto& t : apply_threads_) {
        t->shutdown();
    }

    if (snapshot_manager_ != nullptr) {
        snapshot_manager_.reset(nullptr);
    }

    if (transport_ != nullptr) {
        transport_->Shutdown();
    }

    return Status::OK();
}

Status RaftServerImpl::CreateRaft(const RaftOptions& ops, std::shared_ptr<Raft>* raft) {
    auto status = ops.Validate();
    if (!status.ok()) {
        return status;
    }

    uint64_t counter = 0;
    {
        std::unique_lock<sharkstore::shared_mutex> lock(rafts_mu_);
        auto it = all_rafts_.find(ops.id);
        if (it != all_rafts_.end()) {
            return Status(Status::kDuplicate, "create raft", std::to_string(ops.id));
        }
        auto ret = creating_rafts_.insert(ops.id);
        if (!ret.second) {
            return Status(Status::kDuplicate, "raft is creating", std::to_string(ops.id));
        }
        counter = create_count_++;
    }

    RaftContext ctx;
    ctx.msg_sender = transport_.get();
    ctx.snapshot_manager = snapshot_manager_.get();
    ctx.consensus_thread = consensus_threads_[counter % consensus_threads_.size()];
    if (!ops_.apply_in_place) {
        ctx.apply_thread = apply_threads_[counter % apply_threads_.size()];
    }

    std::shared_ptr<RaftImpl> r;
    try {
        r = std::make_shared<RaftImpl>(ops_, ops, ctx);
    } catch (RaftException& e) {
        {
            std::unique_lock<sharkstore::shared_mutex> lock(rafts_mu_);
            creating_rafts_.erase(ops.id);
        }
        return Status(Status::kUnknown, "create raft", e.what());
    }

    assert(r != nullptr);
    {
        std::unique_lock<sharkstore::shared_mutex> lock(rafts_mu_);
        all_rafts_.emplace(ops.id, r);
        creating_rafts_.erase(ops.id);
    }
    *raft = std::static_pointer_cast<Raft>(r);

    return Status::OK();
}

Status RaftServerImpl::RemoveRaft(uint64_t id, bool backup) {
    std::shared_ptr<RaftImpl> r;
    {
        std::unique_lock<sharkstore::shared_mutex> lock(rafts_mu_);
        auto it = all_rafts_.find(id);
        if (it != all_rafts_.end()) {
            r = it->second;
            r->Stop();
            all_rafts_.erase(it);
        } else {
            return Status(Status::kNotFound, "remove raft", std::to_string(id));
        }
    }

    if (r) {
        // 备份raft日志
        if (backup) {
            auto s = r->BackupLog();
            if (!s.ok()) {
                return Status(Status::kIOError, "backup raft log", s.ToString());
            }
        }
        // 删除raft日志
        auto s = r->Destroy();
        if (!s.ok()) {
            return Status(Status::kIOError, "remove raft log", s.ToString());
        }
    }
    return Status::OK();
}

std::shared_ptr<RaftImpl> RaftServerImpl::findRaft(uint64_t id) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rafts_mu_);

    auto it = all_rafts_.find(id);
    if (it != all_rafts_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
}

std::shared_ptr<Raft> RaftServerImpl::FindRaft(uint64_t id) const {
    return std::static_pointer_cast<Raft>(findRaft(id));
}

void RaftServerImpl::GetStatus(ServerStatus* status) const {
    status->total_snap_sending = snapshot_manager_->SendingCount();
    status->total_snap_applying = snapshot_manager_->ApplyingCount();
}

void RaftServerImpl::onMessage(MessagePtr& msg) {
    if (running_) {
        switch (msg->type()) {
            case pb::HEARTBEAT_REQUEST:
                onHeartbeatReq(msg);
                break;
            case pb::HEARTBEAT_RESPONSE:
                onHeartbeatResp(msg);
                break;
            default: {
                auto raft = findRaft(msg->id());
                if (raft) {
                    raft->RecvMsg(msg);
                }
                break;
            }
        }
    }
}

void RaftServerImpl::onHeartbeatReq(MessagePtr& msg) {
    MessagePtr resp(new pb::Message);
    resp->set_type(pb::HEARTBEAT_RESPONSE);
    resp->set_from(ops_.node_id);
    resp->set_to(msg->from());

    const auto& ids = msg->hb_ctx().ids();
    for (auto it = ids.begin(); it != ids.end(); ++it) {
        uint64_t id = *it;
        auto raft = findRaft(id);
        if (raft) {
            resp->mutable_hb_ctx()->add_ids(id);
            MessagePtr sub_msg(new pb::Message);
            sub_msg->set_id(id);
            sub_msg->set_type(msg->type());
            sub_msg->set_from(msg->from());
            sub_msg->set_to(msg->to());
            raft->RecvMsg(sub_msg);
        }
    }

    transport_->SendMessage(resp);
}

void RaftServerImpl::onHeartbeatResp(MessagePtr& msg) {
    const auto& ids = msg->hb_ctx().ids();
    for (auto it = ids.begin(); it != ids.end(); ++it) {
        uint64_t id = *it;
        auto raft = findRaft(id);
        if (raft) {
            MessagePtr sub_msg(new pb::Message);
            sub_msg->set_id(id);
            sub_msg->set_type(msg->type());
            sub_msg->set_from(msg->from());
            sub_msg->set_to(msg->to());
            raft->RecvMsg(sub_msg);
        }
    }
}

void RaftServerImpl::sendHeartbeat(const RaftMapType& rafts) {
    std::map<uint64_t, std::set<uint64_t>> ctxs;

    for (auto& kv : rafts) {
        auto& r = kv.second;
        if (r->IsLeader()) {
            std::vector<Peer> peers;
            r->GetPeers(&peers);
            for (auto& p : peers) {
                if (p.node_id == ops_.node_id) {
                    continue;
                }
                ctxs[p.node_id].insert(kv.first);
            }
        }
    }

    for (auto& kv : ctxs) {
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::HEARTBEAT_REQUEST);
        msg->set_to(kv.first);
        msg->set_from(ops_.node_id);
        for (auto id : kv.second) {
            msg->mutable_hb_ctx()->add_ids(id);
        }
        transport_->SendMessage(msg);
    }
}

void RaftServerImpl::stepTick(const RaftMapType& rafts) {
    assert(tick_msg_->type() == pb::LOCAL_MSG_TICK);
    for (auto& r : rafts) {
        r.second->Tick(tick_msg_);
    }
}

void RaftServerImpl::tickRoutine() {
    while (running_) {
        std::this_thread::sleep_for(ops_.tick_interval);

        RaftMapType rafts;
        {
            std::lock_guard<sharkstore::shared_mutex> lock(rafts_mu_);
            rafts = all_rafts_;
        }
        sendHeartbeat(rafts);
        stepTick(rafts);
        printMetrics();
    }
}

void RaftServerImpl::printMetrics() {
    static time_t last = time(NULL);
    time_t now = time(NULL);
    if (now > last && now - last > 10) {
        last = now;

        // print consensus queue size
        std::string consensus_metrics = "[";
        for (size_t i = 0; i < consensus_threads_.size(); ++i) {
            consensus_metrics += std::to_string(consensus_threads_[i]->size());
            if (i != consensus_threads_.size() - 1) {
                consensus_metrics += ", ";
            }
        }
        consensus_metrics += "]";
        LOG_INFO("raft[metric] consensus queue size: %s", consensus_metrics.c_str());

        // print apply queue size
        if (!ops_.apply_in_place) {
            std::string apply_metrics = "[";
            for (size_t i = 0; i < apply_threads_.size(); ++i) {
                apply_metrics += std::to_string(apply_threads_[i]->size());
                if (i != apply_threads_.size() - 1) {
                    apply_metrics += ", ";
                }
            }
            apply_metrics += "]";
            LOG_INFO("raft[metric] apply queue size: %s", apply_metrics.c_str());
        }
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
