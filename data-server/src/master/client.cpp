#include "client.h"

#include <thread>

#include "frame/sf_logger.h"
#include "rpc_types.h"

namespace sharkstore {
namespace dataserver {
namespace master {

Client::Client(const std::vector<std::string>& ms_addrs)
    : ms_addrs_(ms_addrs.cbegin(), ms_addrs.cend()),
      last_async_ask_(std::chrono::steady_clock::now()) {
    if (ms_addrs.empty()) {
        throw std::runtime_error("invalid master server addresses(empty).");
    }
}

Client::~Client() { cq_.Shutdown(); }

Status Client::GetNodeID(const mspb::GetNodeIdRequest& req, uint64_t* node_id,
                         bool* clearup) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    grpc::ClientContext context;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(kRpcTimeoutMs);
    context.set_deadline(deadline);

    mspb::GetNodeIdResponse resp;
    auto status = conn->GetStub()->GetNodeId(&context, req, &resp);
    checkStatus(status);
    if (!status.ok()) {
        return Status(Status::kUnknown, status.error_message() + ", code=" +
                                            std::to_string(status.error_code()),
                      conn->GetAddr());
    } else if (checkResponseError(conn->GetAddr(), resp.header())) {
        return Status(Status::kNotLeader, "", conn->GetAddr());
    } else {
        *node_id = resp.node_id();
        *clearup = resp.clearup();
        return Status::OK();
    }
}

Status Client::NodeLogin(uint64_t node_id) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    grpc::ClientContext context;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(kRpcTimeoutMs);
    context.set_deadline(deadline);

    mspb::NodeLoginRequest req;
    req.set_node_id(node_id);
    mspb::NodeLoginResponse resp;
    auto status = conn->GetStub()->NodeLogin(&context, req, &resp);
    checkStatus(status);
    if (!status.ok()) {
        return Status(Status::kUnknown, status.error_message() + ", code=" +
                                            std::to_string(status.error_code()),
                      conn->GetAddr());
    } else if (checkResponseError(conn->GetAddr(), resp.header())) {
        return Status(Status::kNotLeader, "", conn->GetAddr());
    } else {
        return Status::OK();
    }
}

Status Client::GetNodeAddress(uint64_t node_id, std::string* server_addr,
                              std::string* raft_addr, std::string* http_addr) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    mspb::GetNodeRequest req;
    req.set_id(node_id);

    grpc::ClientContext context;
    mspb::GetNodeResponse resp;
    auto status = conn->GetStub()->GetNode(&context, req, &resp);
    checkStatus(status);
    if (!status.ok()) {
        return Status(Status::kUnknown, status.error_message() + ", code=" +
                                            std::to_string(status.error_code()),
                      conn->GetAddr());
    } else if (checkResponseError(conn->GetAddr(), resp.header())) {
        return Status(Status::kNotLeader, "", conn->GetAddr());
    } else {
        if (server_addr) *server_addr = resp.node().server_addr();
        if (raft_addr) *raft_addr = resp.node().raft_addr();
        if (http_addr) *http_addr = resp.node().http_addr();
        return Status::OK();
    }
}

void Client::Start(TaskHandler* handler) {
    std::thread thr(std::bind(&Client::recvResponse, this, handler));
    thr.detach();
}

Status Client::AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest& req) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    auto call = new AsyncCallResultT<mspb::NodeHeartbeatResponse>;
    call->type = AsyncCallType::kNodeHeartbeat;
    call->response_reader =
        conn->GetStub()->AsyncNodeHeartbeat(&call->context, req, &cq_);
    call->Finish();
    return Status::OK();
}

Status Client::AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest& req) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    auto call = new AsyncCallResultT<mspb::RangeHeartbeatResponse>;
    call->type = AsyncCallType::kRangeHeartbeat;
    call->response_reader =
        conn->GetStub()->AsyncRangeHeartbeat(&call->context, req, &cq_);
    call->Finish();
    return Status::OK();
}

Status Client::AsyncAskSplit(const mspb::AskSplitRequest& req) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    auto call = new AsyncCallResultT<mspb::AskSplitResponse>;
    call->type = AsyncCallType::kAskSplit;
    call->response_reader = conn->GetStub()->AsyncAskSplit(&call->context, req, &cq_);
    call->Finish();
    return Status::OK();
}

Status Client::AsyncReportSplit(const mspb::ReportSplitRequest& req) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    auto call = new AsyncCallResultT<mspb::ReportSplitResponse>;
    call->type = AsyncCallType::kReportSplit;
    call->response_reader = conn->GetStub()->AsyncReportSplit(&call->context, req, &cq_);
    call->Finish();
    return Status::OK();
}

void Client::set_leader(const std::string& leader) {
    FLOG_INFO("[Master] Leader set to %s.", leader.c_str());

    std::lock_guard<std::mutex> lock(addr_mu_);
    leader_ = leader;
    if (!leader.empty()) {
        addAddr(leader);
    }
}

std::string Client::leader() const {
    std::lock_guard<std::mutex> lock(addr_mu_);
    return leader_;
}

std::string Client::candidate() {
    std::lock_guard<std::mutex> lock(addr_mu_);
    assert(!ms_addrs_.empty());
    size_t idx = (magic_counter_++) % ms_addrs_.size();
    return ms_addrs_[idx];
}

void Client::addAddr(const std::string& addr) {
    assert(!addr.empty());
    auto it = std::find(ms_addrs_.begin(), ms_addrs_.end(), addr);
    if (it == ms_addrs_.end()) {
        ms_addrs_.push_back(addr);
    }
}

void Client::updateLeader(const std::string& from,
                          const mspb::GetMSLeaderResponse& resp) {
    if (resp.has_leader()) {
        FLOG_INFO("[Master] GetMSLeader resp from %s report new leader=%s.", from.c_str(),
                  resp.leader().address().c_str());

        set_leader(resp.leader().address());
    } else {
        FLOG_WARN("[Master] GetMSLeader resp from %s leader not set.", from.c_str());
    }
}

void Client::askLeader() {
    mspb::GetMSLeaderRequest req;
    mspb::GetMSLeaderResponse resp;
    grpc::ClientContext context;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(kRpcTimeoutMs);
    context.set_deadline(deadline);

    std::string addr = this->candidate();
    auto conn = getConnection(addr);
    FLOG_DEBUG("[Master] ask leader to addr %s.", addr.c_str());

    auto status = conn->GetStub()->GetMSLeader(&context, req, &resp);
    if (!status.ok()) {
        FLOG_ERROR("[Master] call GetMSLeader to %s failed. code=%d, errmsg=%s.",
                   addr.c_str(), status.error_code(), status.error_message().c_str());
    } else if (!checkResponseError(addr, resp.header())) {
        updateLeader(addr, resp);
    }
}

void Client::asyncAskLeader() {
    // avoid ask too frequently
    static const int kMinAsyncAskIntervalSec = 3;

    auto now = std::chrono::steady_clock::now();
    auto elapse_secs =
        std::chrono::duration_cast<std::chrono::seconds>(now - last_async_ask_).count();
    if (elapse_secs <= kMinAsyncAskIntervalSec) {
        return;
    } else {
        last_async_ask_ = now;
    }

    std::string addr = this->candidate();
    auto conn = getConnection(addr);
    FLOG_DEBUG("[Master] async ask leader to addr %s.", addr.c_str());

    mspb::GetMSLeaderRequest req;

    auto call = new AsyncCallResultT<mspb::GetMSLeaderResponse>;
    call->type = AsyncCallType::kGetMSLeader;
    call->response_reader = conn->GetStub()->AsyncGetMSLeader(&call->context, req, &cq_);
    call->Finish();
}

Client::ConnPtr Client::getConnection(const std::string& addr) {
    assert(!addr.empty());

    std::lock_guard<std::mutex> lock(conn_mu_);
    auto it = connections_.find(addr);
    if (it != connections_.end()) {
        return it->second;
    } else {
        auto conn = std::make_shared<Connection>(addr, cq_);
        connections_.emplace(addr, conn);
        return conn;
    }
}

Client::ConnPtr Client::getLeaderConn() {
    auto leader_addr = leader();
    // 没leader先尝试问一次leader
    if (leader_addr.empty()) {
        askLeader();
        leader_addr = leader();
    }
    // 还是没leader就报错
    if (leader_addr.empty()) {
        FLOG_WARN("[Master] no ms leader current time.");
        return nullptr;
    } else {
        return getConnection(leader_addr);
    }
}

void Client::recvResponse(TaskHandler* handler) {
    void* tag = nullptr;
    bool ok = false;
    while (true) {
        tag = nullptr;
        ok = false;
        cq_.Next(&tag, &ok);
        if (!ok) {
            return;
        }

        AsyncCallResult* call = static_cast<AsyncCallResult*>(tag);
        if (call->type != AsyncCallType::kGetMSLeader) {
            checkStatus(call->status);
        }

        std::string from = call->context.peer();
        if (!call->status.ok()) {
            FLOG_ERROR("[Master] rpc failed to %s. type=%s, code=%d, msg=%s.",
                       from.c_str(), AsyncCallTypeName(call->type).c_str(),
                       call->status.error_code(), call->status.error_message().c_str());
        } else {
            FLOG_DEBUG("[Master] recv %s respone from %s.",
                       AsyncCallTypeName(call->type).c_str(), from.c_str());
            dispatchResponse(handler, from, call);
        }
        delete call;
    }
}

void Client::dispatchResponse(TaskHandler* handler, const std::string& from,
                              AsyncCallResult* call) {
    switch (call->type) {
        case AsyncCallType::kAskSplit: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::AskSplitResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                handler->OnAskSplitResp(res->response);
            }
            break;
        }
        case AsyncCallType::kReportSplit: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::ReportSplitResponse>*>(call);
            checkResponseError(from, res->response.header());
            break;
        }
        case AsyncCallType::kNodeHeartbeat: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::NodeHeartbeatResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                handler->OnNodeHeartbeatResp(res->response);
            }
            break;
        }
        case AsyncCallType::kRangeHeartbeat: {
            auto res =
                dynamic_cast<AsyncCallResultT<mspb::RangeHeartbeatResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                handler->OnRangeHeartbeatResp(res->response);
            }
            break;
        }
        case AsyncCallType::kGetMSLeader: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::GetMSLeaderResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                updateLeader(from, res->response);
            }
            break;
        }
        default:
            FLOG_ERROR("[Master] recv unkown response type=%s from %s.",
                       AsyncCallTypeName(call->type).c_str(), from.c_str());
    }
}

void Client::checkStatus(const grpc::Status& s) {
    static const uint64_t kAskThreshold = 5;

    if (s.error_code() == grpc::DEADLINE_EXCEEDED ||
        s.error_code() == grpc::UNAVAILABLE) {
        if (++network_fail_count_ == kAskThreshold) {
            asyncAskLeader();
            network_fail_count_ = 0;
        }
    } else if (s.ok()) {
        network_fail_count_ = 0;
    }
}

bool Client::checkResponseError(const std::string& from,
                                const mspb::ResponseHeader& header) {
    if (header.has_error()) {
        const auto& error = header.error();
        if (error.has_new_leader()) {
            FLOG_INFO("[Master] resp header from %s report new leader=%s.", from.c_str(),
                      error.new_leader().address().c_str());

            set_leader(error.new_leader().address());
        } else if (error.has_no_leader()) {
            FLOG_WARN("[Master] resp header from %s report no leader.", from.c_str());

            set_leader("");
        }
        return true;
    } else {
        return false;
    }
}

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
