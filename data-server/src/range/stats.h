_Pragma("once");

#include "monitor/statistics.h"

namespace sharkstore {
namespace dataserver {
namespace range {

class RangeStats {
public:
    RangeStats() = default;
    virtual ~RangeStats() = default;

    virtual void PushTime(monitor::HistogramType type, int64_t time) {}
    virtual void IncrSplitCount() {}
    virtual void DecrSplitCount() {}

    // leader变更时，上报当前range是否是leader，用来统计节点leader情况
    virtual void ReportLeader(uint64_t range_id, bool is_leader) {}
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
