#include "server.h"

#include <common/ds_config.h>
#include <iostream>

#include "common/ds_config.h"
#include "common/socket_session_impl.h"
#include "frame/sf_logger.h"

#include "version.h"
#include "manager.h"
#include "master/worker.h"
#include "node_address.h"
#include "raft_logger.h"
#include "range_server.h"
#include "run_status.h"
#include "worker.h"

namespace sharkstore {
namespace dataserver {
namespace server {

DataServer::DataServer() {
    context_ = new ContextServer;

    context_->worker = new Worker;
    context_->manager = new Manager;

    context_->run_status = new RunStatus;
    context_->range_server = new RangeServer;
    context_->socket_session = new common::SocketSessionImpl;

    // create master worker
    std::vector<std::string> ms_addrs;
    for (int i = 0; i < ds_config.hb_config.master_num; i++) {
        if (ds_config.hb_config.master_host[i][0] != '\0') {
            ms_addrs.push_back(ds_config.hb_config.master_host[i]);
        }
    }
    context_->master_worker =
        new master::Worker(ms_addrs, ds_config.hb_config.node_interval);
}

DataServer::~DataServer() {
    if (context_->worker != nullptr) {
        delete context_->worker;
    }

    if (context_->manager != nullptr) {
        delete context_->manager;
    }

    if (context_->master_worker != nullptr) {
        delete context_->master_worker;
    }

    if (context_->run_status != nullptr) {
        delete context_->run_status;
    }

    if (context_->range_server != nullptr) {
        delete context_->range_server;
    }

    if (context_->socket_session != nullptr) {
        delete context_->socket_session;
    }

    if (context_->raft_server != nullptr) {
        delete context_->raft_server;
    }

    delete context_;
}

bool DataServer::startRaftServer() {
    raft::SetLogger(new RaftLogger());

    // 初始化 raft server
    raft::RaftServerOptions ops;

    ops.node_id = context_->node_id;
    ops.consensus_threads_num = ds_config.raft_config.consensus_threads;
    ops.consensus_queue_capacity = ds_config.raft_config.consensus_queue;
    ops.apply_threads_num = ds_config.raft_config.apply_threads;
    ops.apply_queue_capacity = ds_config.raft_config.apply_queue;
    ops.tick_interval =
        std::chrono::milliseconds(ds_config.raft_config.tick_interval_ms);
    ops.max_size_per_msg = ds_config.raft_config.max_msg_size;

    ops.transport_options.listen_port = ds_config.raft_config.port;
    ops.transport_options.send_io_threads =
        ds_config.raft_config.transport_send_threads;
    ops.transport_options.recv_io_threads =
        ds_config.raft_config.transport_recv_threads;
    ops.transport_options.resolver = std::make_shared<NodeAddress>();

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
    auto s = context_->master_worker->GetNodeId(
        ds_config.raft_config.port, ds_config.worker_config.port,
        ds_config.manager_config.port, version, &node_id, &clearup);

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

    if (context_->manager->Init(context_) != 0) {
        return -1;
    }

    if (context_->run_status->Init(context_) != 0) {
        return -1;
    }

    return 0;
}

int DataServer::Start() {
    if (Init() != 0) {
        return -1;
    }

    if (context_->worker->Start() != 0) {
        return -1;
    }

    if (context_->manager->Start() != 0) {
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
    if (context_->worker != nullptr) {
        context_->worker->Stop();
    }
    if (context_->manager != nullptr) {
        context_->manager->Stop();
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
