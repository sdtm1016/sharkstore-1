_Pragma("once");

#include <atomic>
#include "types.h"

namespace sharkstore {
namespace raft {
namespace impl {

class SnapTask {
public:
    explicit SnapTask(const SnapContext& ctx) : context_(ctx) {}
    virtual ~SnapTask() = default;

    SnapTask(const SnapTask&) = delete;
    SnapTask& operator=(const SnapTask&) = delete;

    // 任务是否已经分发，避免重复分发(分发：投递给了worker）
    bool IsDispatched() const { return dispatched_; }
    void MarkAsDispatched() { dispatched_ = true; }

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

protected:
    virtual void run(SnapResult *result) = 0;

private:
    const SnapContext context_;
    std::atomic<bool> dispatched_ = {false};
    SnapReporter reporter_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
