_Pragma("once");

#include "proto/gen/mspb.grpc.pb.h"

namespace sharkstore {
namespace dataserver {
namespace master {

// TasHandler 处理master的response以及提供node心跳数据
class TaskHandler {
public:
    virtual ~TaskHandler() {}

    virtual void OnNodeHeartbeatResp(const mspb::NodeHeartbeatResponse&) = 0;
    virtual void OnRangeHeartbeatResp(const mspb::RangeHeartbeatResponse&) = 0;
    virtual void OnAskSplitResp(const mspb::AskSplitResponse&) = 0;

    virtual void CollectNodeHeartbeat(mspb::NodeHeartbeatRequest* req) = 0;
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
