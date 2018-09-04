_Pragma("once");

#include "server/context_server.h"
#include "proto/gen/ds_admin.pb.h"
#include "net/server.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

class AdminServer {
public:
    explicit AdminServer(server::ContextServer* context);
    ~AdminServer();

    Status Start(uint16_t port);
    Status Stop();

    AdminServer(const AdminServer&) = delete;
    AdminServer& operator=(const AdminServer&) = delete;

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);

    Status checkAuth(const ds_adminpb::AdminAuth& auth);
    Status execute(const ds_adminpb::AdminRequest& req, ds_adminpb::AdminResponse* resp);

    Status setConfig(const ds_adminpb::SetConfigRequest& req, ds_adminpb::SetConfigResponse* resp);
    Status getConfig(const ds_adminpb::GetConfigRequest& req, ds_adminpb::GetConfigResponse* resp);
    Status getInfo(const ds_adminpb::GetInfoRequest& req, ds_adminpb::GetInfoResponse* resp);
    Status forceSplit(const ds_adminpb::ForceSplitRequest& req, ds_adminpb::ForceSplitResponse* resp);
    Status compaction(const ds_adminpb::CompactionRequest& req, ds_adminpb::CompactionResponse* resp);
    Status clearQueue(const ds_adminpb::ClearQueueRequest& req, ds_adminpb::ClearQueueResponse* resp);
    Status getPending(const ds_adminpb::GetPendingsRequest& req, ds_adminpb::GetPendingsResponse* resp);
    Status flushDB(const ds_adminpb::FlushDBRequest& req, ds_adminpb::FlushDBResponse* resp);

private:
    server::ContextServer* context_ = nullptr;
    std::unique_ptr<net::Server> net_server_;
    // TODO: worker thread
};

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
