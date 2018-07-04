#include "manager.h"

#include "send_task.h"
#include "apply_task.h"
#include "worker_pool.h"

namespace sharkstore {
namespace raft {
namespace impl {

SnapshotManager::SnapshotManager(const SnapshotOptions& opt)
    : opt_(opt),
      send_work_pool_(new SnapWorkerPool("snap_send", opt_.max_send_concurrency)),
      apply_work_pool_(new SnapWorkerPool("snap_apply", opt_.max_apply_concurrency)) {}

SnapshotManager::~SnapshotManager() = default;

Status SnapshotManager::Dispatch(const std::shared_ptr<SendSnapTask>& send_task) {
    if(send_work_pool_->Post(send_task)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy, "max send snapshot concurrency limit reached",
                      std::to_string(send_work_pool_->Runnings()));
    }
}

Status SnapshotManager::Dispatch(const std::shared_ptr<ApplySnapTask>& apply_task) {
    if(apply_work_pool_->Post(apply_task)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy, "max apply snapshot concurrency limit reached",
                      std::to_string(apply_work_pool_->Runnings()));
    }
}

uint64_t SnapshotManager::SendingCount() const {
    return send_work_pool_->Runnings();

}

uint64_t SnapshotManager::ApplyingCount() const {
    return apply_work_pool_->Runnings();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
