_Pragma("once");

#include <atomic>
#include <memory>

#include "types.h"

namespace sharkstore {
namespace raft {
namespace impl {

class SnapTask {
public:
    explicit SnapTask(const SnapContext& ctx) :
            context_(ctx),
            id_(std::to_string(ctx.id) + "/" + std::to_string(ctx.term) + "/" + std::to_string(ctx.uuid))
    {
    }

    virtual ~SnapTask() = default;

    SnapTask(const SnapTask&) = delete;
    SnapTask& operator=(const SnapTask&) = delete;

    // 任务是否已经分发，避免重复分发(分发：投递给了worker）
    bool IsDispatched() const { return dispatched_; }
    void MarkAsDispatched() { dispatched_ = true; }

    const std::string& ID() const { return id_; }

    const SnapContext& GetContext() const { return context_; }

    // 设置结果报告回调，Run之前设置
    void SetReporter(const SnapReporter& reporter) { reporter_ = reporter; }

    void Run() {
        assert(reporter_);

        SnapResult result;
        this->run(&result);
        reporter_(context_, result);
    }

    virtual void Cancel() = 0;

    virtual std::string Description() const = 0;

protected:
    virtual void run(SnapResult *result) = 0;

private:
    const SnapContext context_;
    const std::string id_; // unique task id

    std::atomic<bool> dispatched_ = {false};
    SnapReporter reporter_;
};

using SnapTaskPtr = std::shared_ptr<SnapTask>;

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
