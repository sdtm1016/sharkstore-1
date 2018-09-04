#include <common/ds_config.h>
#include "range_context_impl.h"

#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "range_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class DefaultSplitPolicy : public range::SplitPolicy {
public:
    bool IsEnabled() const override { return true; }

    std::string Description() const override {
        return std::string("Default/") + range::SplitKeyModeName(KeyMode());
    }

    uint64_t CheckSize() const override {
        return ds_config.range_config.check_size;
    }

    uint64_t SplitSize() const override {
        return ds_config.range_config.split_size;
    }

    uint64_t MaxSize() const override {
        return ds_config.range_config.max_size;
    }

    range::SplitKeyMode KeyMode() const override {
        switch (ds_config.range_config.access_mode) {
            case 0:
                return range::SplitKeyMode::kNormal;
            case 1:
                return range::SplitKeyMode::kRedis;
            case 2:
                return range::SplitKeyMode::kLockWatch;
            default:
                return range::SplitKeyMode::kInvalid;
        }
    }
};

RangeContextImpl::RangeContextImpl(ContextServer *s) :
    server_(s),
    split_policy_(new DefaultSplitPolicy) {
}

uint64_t RangeContextImpl::GetFSUsagePercent() const {
    return server_->run_status->GetFilesystemUsedPercent();
}

void RangeContextImpl::ScheduleHeartbeat(uint64_t range_id, bool delay) {
    auto expire = delay ? ds_config.hb_config.range_interval * 1000 + getticks() :
            getticks();
    server_->range_server->LeaderQueuePush(range_id, expire);
}

void RangeContextImpl::ScheduleCheckSize(uint64_t range_id) {
    server_->range_server->StatisPush(range_id);
}

std::shared_ptr<range::Range> RangeContextImpl::FindRange(uint64_t range_id) {
    return server_->range_server->Find(range_id);
}

// split
Status RangeContextImpl::SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req,
                  uint64_t raft_index) {
    return server_->range_server->SplitRange(range_id, req, raft_index);
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
