#include "master_worker_mock.h"



namespace sharkstore {
namespace test {
namespace mock {

Status MasterWorkerMock::GetNodeId(mspb::GetNodeIdRequest& req,
                 uint64_t *node_id, bool *clearup) {
    *node_id = 1;
    *clearup = false;
    return Status::OK();
}

Status MasterWorkerMock::NodeLogin(uint64_t node_id) {
    return Status::OK();
}

Status MasterWorkerMock::Start(master::TaskHandler *handler) {
    return Status::OK();
}

void MasterWorkerMock::Stop() {}

Status MasterWorkerMock::GetRaftAddress(uint64_t node_id, std::string *addr) {
    return Status(Status::kNotSupported);
}

void MasterWorkerMock::AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest &req) {
}

void MasterWorkerMock::AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest &req) {
}

void MasterWorkerMock::AsyncAskSplit(const mspb::AskSplitRequest &req) {
}

void MasterWorkerMock::AsyncReportSplit(const mspb::ReportSplitRequest &req) {
}

}
}
}
