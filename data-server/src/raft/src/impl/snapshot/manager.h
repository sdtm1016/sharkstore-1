_Pragma("once");

#include "raft/include/raft/options.h"

namespace sharkstore {
namespace raft {
namespace impl {

class SnapWorkerPool;
class SendSnapTask;
class ApplySnapTask;

class SnapshotManager final {
public:
    explicit SnapshotManager(const SnapshotOptions& opt);
    ~SnapshotManager();

    SnapshotManager(const SnapshotManager&) = delete;
    SnapshotManager& operator=(const SnapshotManager&) = delete;

    Status Dispatch(const std::shared_ptr<SendSnapTask>& send_task);
    Status Dispatch(const std::shared_ptr<ApplySnapTask>& apply_task);

    uint64_t SendingCount() const;
    uint64_t ApplyingCount() const;

    std::string GetSendingDesc() const;
    std::string GetApplyingDesc() const;

private:
    const SnapshotOptions opt_;

    std::unique_ptr<SnapWorkerPool> send_work_pool_;
    std::unique_ptr<SnapWorkerPool> apply_work_pool_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
