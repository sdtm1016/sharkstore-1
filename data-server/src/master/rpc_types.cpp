#include "rpc_types.h"

#include <chrono>

namespace sharkstore {
namespace dataserver {
namespace master {

AsyncCallResult::AsyncCallResult() {
    auto deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(kRpcTimeoutMs);

    context.set_deadline(deadline);
}

std::string AsyncCallTypeName(AsyncCallType type) {
    switch (type) {
        case AsyncCallType::kInvalid:
            return "Invalid";
        case AsyncCallType::kAskSplit:
            return "AskSplit";
        case AsyncCallType::kReportSplit:
            return "ReportSplit";
        case AsyncCallType::kNodeHeartbeat:
            return "NodeHeartbeat";
        case AsyncCallType::kRangeHeartbeat:
            return "RangeHeartbeat";
        case AsyncCallType::kGetMSLeader:
            return "GetMSLeader";
        default:
            return "Unknown";
    }
}

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
