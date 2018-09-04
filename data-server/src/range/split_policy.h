_Pragma("once");

#include <string>
#include <cstdint>
#include <memory>

namespace sharkstore {
namespace dataserver {
namespace range {

enum class SplitKeyMode {
    kNormal = 0,
    kRedis = 1,
    kLockWatch = 2,

    kInvalid = 255,
};

std::string SplitKeyModeName(SplitKeyMode mode);

class SplitPolicy {
public:
    SplitPolicy() = default;
    virtual ~SplitPolicy() = default;

    virtual bool IsEnabled() const = 0;
    virtual std::string Description() const = 0;

    virtual uint64_t CheckSize() const = 0;
    virtual uint64_t SplitSize() const = 0;
    virtual uint64_t MaxSize() const = 0;

    virtual SplitKeyMode KeyMode() const = 0;
};

std::unique_ptr<SplitPolicy> NewDisableSplitPolicy();

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
