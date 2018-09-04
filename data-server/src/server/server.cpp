#include "server.h"

#include <common/ds_config.h>
#include <iostream>

#include "common/ds_config.h"
#include "common/socket_session_impl.h"

#include "master/worker_impl.h"
#include "admin/admin_server.h"

#include "node_address.h"
#include "raft_logger.h"
#include "range_server.h"
#include "run_status.h"
#include "version.h"
#include "worker.h"

namespace sharkstore {
namespace dataserver {
namespace server {

DataServer::DataServer() {
    context_ = new ContextServer;

    context_->worker = new Worker;

    context_->run_status = new RunStatus;
    context_->range_server = new RangeServer;
    context_->socket_session = new common::SocketSessionImpl;

    // create master worker
    std::vector<std::string> ms_addrs;
    for (int i = 0; i < ds_config.hb_config.master_num; i++) {
        if (ds_config.hb_config.master_host[i][0] != '\0') {
            ms_addrs.emplace_back(ds_config.hb_config.master_host[i]);
        }
    }
    context_->master_worker =
        new master::WorkerImpl(ms_addrs, ds_config.hb_config.node_interval);
}

DataServer::~DataServer() {
    delete context_->worker;
    delete context_->master_worker;
    delete context_->run_status;
    delete context_->range_server;
    delete context_->socket_session;
    delete context_->raft_server;
    delete context_;
}

bool DataServer::startRaftServer() {
    print_raft_config();

    raft::SetLogger(new RaftLogger());

    // 初始化 raft server
    raft::RaftServerOptions ops;

    ops.node_id = context_->node_id;
    ops.consensus_threads_num =
        static_cast<uint8_t>(ds_config.raft_config.consensus_threads);
    ops.consensus_queue_capacity = ds_config.raft_config.consensus_queue;
    ops.apply_threads_num = static_cast<uint8_t>(ds_config.raft_config.apply_threads);
    ops.apply_queue_capacity = ds_config.raft_config.apply_queue;
    ops.tick_interval = std::chrono::milliseconds(ds_config.raft_config.tick_interval_ms);
    ops.max_size_per_msg = ds_config.raft_config.max_msg_size;

    ops.transport_options.listen_port = static_cast<uint16_t>(ds_config.raft_config.port);
    ops.transport_options.send_io_threads = ds_config.raft_config.transport_send_threads;
    ops.transport_options.recv_io_threads = ds_config.raft_config.transport_recv_threads;
    ops.transport_options.resolver =
        std::make_shared<NodeAddress>(context_->master_worker);

    auto rs = raft::CreateRaftServer(ops);
    context_->raft_server = rs.release();
    auto s = context_->raft_server->Start();
    if (!s.ok()) {
        FLOG_ERROR("RaftServer Start error ... : %s", s.ToString().c_str());
        return false;
    }

    FLOG_DEBUG("RaftServer Started...");
    return true;
}

int DataServer::Init() {
    std::string version = GetGitDescribe();
    FLOG_INFO("Version: %s", version.c_str());

    // GetNodeId from master server
    bool clearup = false;
    uint64_t node_id = 0;
    mspb::GetNodeIdRequest req;
    req.set_server_port(static_cast<uint32_t>(ds_config.worker_config.port));
    req.set_raft_port(static_cast<uint32_t>(ds_config.raft_config.port));
    req.set_admin_port(static_cast<uint32_t>(ds_config.manager_config.port));
    req.set_version(version);
    auto s = context_->master_worker->GetNodeId(req, &node_id, &clearup);
    if (!s.ok()) {
        FLOG_ERROR("GetNodeId failed. %s", s.ToString().c_str());
        return -1;
    }
    context_->node_id = node_id;

    if (clearup) {
        context_->range_server->Clear();
    }

    if (!startRaftServer()) {
        return -1;
    }

    if (context_->range_server->Init(context_) != 0) {
        return -1;
    }

    if (context_->worker->Init(context_) != 0) {
        return -1;
    }

    if (context_->run_status->Init(context_) != 0) {
        return -1;
    }

    admin_server_.reset(new admin::AdminServer(context_));

    return 0;
}

int DataServer::Start() {
    if (Init() != 0) {
        return -1;
    }

    if (context_->worker->Start() != 0) {
        return -1;
    }

    auto ret = admin_server_->Start(ds_config.manager_config.port);
    if (!ret.ok()) {
        FLOG_ERROR("start admin server failed: %s", ret.ToString().c_str());
        return -1;
    }

    if (context_->range_server->Start() != 0) {
        return -1;
    }

    auto s = context_->master_worker->Start(context_->range_server);
    if (!s.ok()) {
        FLOG_ERROR("start master worker failed. %s", s.ToString().c_str());
        return -1;
    }

    if (context_->run_status->Start() != 0) {
        return -1;
    }

    s = context_->master_worker->NodeLogin(context_->node_id);
    if (!s.ok()) {
        FLOG_ERROR("NodeLogin failed. %s", s.ToString().c_str());
        return -1;
    }
    return 0;
}

void DataServer::Stop() {
    if (admin_server_) {
        admin_server_->Stop();
    }

    if (context_->worker != nullptr) {
        context_->worker->Stop();
    }
    if (context_->range_server != nullptr) {
        context_->range_server->Stop();
    }
    if (context_->raft_server != nullptr) {
        context_->raft_server->Stop();
    }
    if (context_->master_worker != nullptr) {
        context_->master_worker->Stop();
    }
    if (context_->run_status != nullptr) {
        context_->run_status->Stop();
    }
}

void DataServer::DealTask(common::ProtoMessage *task) {
    context_->range_server->DealTask(task);
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
