#ifndef __CONTEXT_SERVER_H__
#define __CONTEXT_SERVER_H__

#include <rocksdb/db.h>

#include "common/socket_session.h"
#include "raft/server.h"

namespace sharkstore {
namespace dataserver {

namespace storage {
class MetaStore;
}

namespace master {
class Worker;
}

namespace server {

class Worker;
class Manager;
class GrpcWorker;
class RunStatus;
class RangeServer;
class RunStatus;

struct ContextServer {
    uint64_t node_id = 0;

    Worker *worker = nullptr;
    Manager *manager = nullptr;

    RunStatus *run_status = nullptr;
    RangeServer *range_server = nullptr;
    common::SocketSession *socket_session = nullptr;
    master::Worker *master_worker = nullptr;

    rocksdb::DB *rocks_db = nullptr;
    std::shared_ptr<rocksdb::Cache> block_cache;  // rocksdb block cache
    std::shared_ptr<rocksdb::Cache> row_cache; // rocksdb row cache
    storage::MetaStore *meta_store = nullptr;

    raft::RaftServer *raft_server = nullptr;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore

#endif  // __CONTEXT_SERVER_H__
