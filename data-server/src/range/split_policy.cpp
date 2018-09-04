#include "split_policy.h"

namespace sharkstore {
namespace dataserver {
namespace range {

std::string SplitKeyModeName(SplitKeyMode mode) {
    switch (mode) {
        case SplitKeyMode::kNormal:
            return "normal";
        case SplitKeyMode::kRedis:
            return "redis";
        case SplitKeyMode::kLockWatch:
            return "lock-watch";
        default:
            return "<unknown>";
    };
}

class DisableSplitPolicy : public SplitPolicy {
public:
    std::string Description() const override { return "DisableSplit"; }
    bool IsEnabled() const override { return false; }
    uint64_t CheckSize() const override { return 0; }
    uint64_t SplitSize() const override { return 0; }
    uint64_t MaxSize() const override { return 0; }
    SplitKeyMode KeyMode() const override { return SplitKeyMode::kNormal; }
};

std::unique_ptr<SplitPolicy> NewDisableSplitPolicy() {
    return std::unique_ptr<SplitPolicy>(new DisableSplitPolicy);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
