_Pragma("once");

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "raft/include/raft/statemachine.h"
#include "../transport/transport.h"

#include "task.h"
#include "types.h"

namespace sharkstore {
namespace raft {
namespace impl {

class ApplySnapTask : public SnapTask {
public:
    struct Options {
        size_t wait_data_timeout_secs = 0;
    };

    ApplySnapTask(const SnapContext& context,
            const std::shared_ptr<StateMachine>& sm);
    ~ApplySnapTask();

    void SetOptions(const Options& opt) { opt_ = opt; }
    void SetTransport(transport::Transport* transport) { transport_= transport; }

    Status RecvData(const MessagePtr& msg);

    void Cancel() override;
    bool IsCanceled() const { return canceled_; }

private:
    void run(SnapResult* result) override;

    Status waitNextData(MessagePtr* data);
    Status applyData(MessagePtr data, bool &over);
    void sendAck(int64_t seq, bool reject = false);

private:
    std::shared_ptr<StateMachine> sm_;

    transport::Transport *transport_ = nullptr;
    Options opt_;

    std::atomic<bool> canceled_ = {false};
    int64_t prev_seq_ = 0;
    MessagePtr next_data_;
    mutable std::mutex mu_;
    std::condition_variable cv_;

    uint64_t snap_index_ = 0;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
