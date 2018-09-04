#ifndef FBASE_DATASERVER_SERVER_DS_SERVER_H_
#define FBASE_DATASERVER_SERVER_DS_SERVER_H_

#include "context_server.h"

namespace sharkstore {
namespace dataserver {

namespace admin { class AdminServer; }

namespace server {

class DataServer {
public:
    ~DataServer();

    DataServer(const DataServer &) = delete;
    DataServer &operator=(const DataServer &) = delete;

    static DataServer &Instance() {
        static DataServer instance_;
        return instance_;
    };

    int Init();
    int Start();
    void Stop();

    ContextServer *context_server() { return context_; }

    void DealTask(common::ProtoMessage *task);

private:
    DataServer();

    bool startRaftServer();

private:
    ContextServer *context_ = nullptr;
    std::unique_ptr<admin::AdminServer> admin_server_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

#endif /* end of include guard: FBASE_DATASERVER_SERVER_DS_SERVER_H_ */
