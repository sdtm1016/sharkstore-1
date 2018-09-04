_Pragma("once");

#include "base/status.h"

#include "options.h"
#include "status.h"

namespace sharkstore {
namespace raft {

class Raft;

class RaftServer {
public:
    RaftServer() = default;
    virtual ~RaftServer() = default;

    RaftServer(const RaftServer&) = delete;
    RaftServer& operator=(const RaftServer&) = delete;

    virtual Status Start() = 0;
    virtual Status Stop() = 0;

    // 创建raft
    virtual Status CreateRaft(const RaftOptions&, std::shared_ptr<Raft>* raft) = 0;

    // 停止并移除Raft，日志会保留
    virtual Status RemoveRaft(uint64_t id) = 0;

    // 销毁raft，同时会清空所属日志文件
    // backup如果为true清空之前会进行备份
    virtual Status DestroyRaft(uint64_t id, bool backup = false) = 0;

    virtual std::shared_ptr<Raft> FindRaft(uint64_t id) const = 0;

    virtual void GetStatus(ServerStatus* status) const = 0;
};

std::unique_ptr<RaftServer> CreateRaftServer(const RaftServerOptions& ops);

} /* namespace raft */
} /* namespace sharkstore */
