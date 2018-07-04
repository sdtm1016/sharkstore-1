_Pragma("once");

#include <condition_variable>
#include <mutex>
#include "base/status.h"
#include "raft/statemachine.h"

namespace sharkstore {
namespace raft {
namespace test {

class NumberStateMachine : public raft::StateMachine {
public:
    NumberStateMachine(uint64_t node_id) : node_id_(node_id) {}

    Status WaitNumber(uint64_t number, size_t timeout_ms = 5000) {
        std::unique_lock<std::mutex> lock(mu_);
        auto ret = cond_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                  [this, number] { return number_ == number; });
        if (ret) {
            return Status::OK();
        } else {
            return Status(Status::kTimedOut);
        }
    }

    Status Apply(const std::string& cmd, uint64_t index) override {
        auto num = std::stoull(cmd);

        std::cout << "[NODE" << node_id_ << "] apply " << cmd
                  << ", index=" << index << ", num=" << num << std::endl;
        {
            std::lock_guard<std::mutex> lock(mu_);
            // check discontinuous
            if (number_ != 0 && number_ + 1 != num) {
                return Status(
                    Status::kCorruption, "discontinuous number",
                    std::to_string(number_) + "-" + std::to_string(num));
            }
            number_ = num;
            applied_ = index;
        }
        cond_.notify_all();
        return Status::OK();
    }

    Status ApplyMemberChange(const ConfChange&, uint64_t) override {
        return Status::OK();
    }

    void OnReplicateError(const std::string& cmd, const Status& status) {}

    virtual void OnLeaderChange(uint64_t leader, uint64_t term) {
        std::cout << "[NODE" << node_id_ << "] leader change to " << leader
                  << " at term " << term << std::endl;
    }

    std::shared_ptr<raft::Snapshot> GetSnapshot() {
        uint64_t number = 0;
        uint64_t applied = 0;
        {
            std::lock_guard<std::mutex> lock(mu_);
            number = number_;
            applied = applied_;
        }
        return std::static_pointer_cast<raft::Snapshot>(
            std::make_shared<Snapshot>(number, applied));
    }

    Status ApplySnapshotStart(const std::string& context) {
        std::cout << "[NODE" << node_id_ << "] start to apply snapshot."
                  << std::endl;
        if (context != std::string(Snapshot::kContext)) {
            return Status(Status::kCorruption, "invalid context", context);
        } else {
            return Status::OK();
        }
    }

    Status ApplySnapshotData(const std::vector<std::string>& datas) {
        if (datas.size() != 1) {
            return Status(Status::kCorruption, "invalid snapshot datas size",
                          std::to_string(datas.size()));
        }

        std::cout << "[NODE" << node_id_
                  << "] apply snapshot data: " << datas[0] << std::endl;

        snaping_number_ = stoull(datas[0]);
        return Status::OK();
    }

    Status ApplySnapshotFinish(uint64_t index) {
        std::cout << "[NODE" << node_id_ << "] finish apply snapshot."
                  << std::endl;

        {
            std::lock_guard<std::mutex> lock(mu_);
            number_ = snaping_number_;
            applied_ = index;
        }
        cond_.notify_all();
        return Status::OK();
    }

private:
    class Snapshot : public raft::Snapshot {
    public:
        Snapshot(uint64_t number, uint64_t applied)
            : number_(number), applied_(applied) {}

        Status Next(std::string* data, bool* over) override {
            data->assign(std::to_string(number_));
            *over = true;
            return Status::OK();
        }

        Status Context(std::string* c) override {
            c->assign(kContext);
            return Status::OK();
        }
        uint64_t ApplyIndex() override { return applied_; }
        void Close() override {}

    public:
        static constexpr const char* kContext = "$number statemachine context$";

    private:
        uint64_t number_;
        uint64_t applied_;
    };

private:
    const uint64_t node_id_ = 0;

    uint64_t number_ = 0;
    uint64_t applied_ = 0;
    uint64_t snaping_number_ = 0;
    std::mutex mu_;
    std::condition_variable cond_;
};

} /* test  */
} /* raft  */
} /* sharkstore  */
