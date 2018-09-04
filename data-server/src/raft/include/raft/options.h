_Pragma("once");

#include <chrono>
#include <memory>
#include <vector>

#include "raft/node_resolver.h"
#include "raft/statemachine.h"

namespace sharkstore {
namespace raft {

struct TransportOptions {
    // 进程内网络传输, 测试
    bool use_inprocess_transport = false;

    // raft通信模块监听ip，默认所有网络接口
    std::string listen_ip = "0.0.0.0";

    // raft通信模块监听端口
    uint16_t listen_port = 0;

    // resolver获取其他dataserver地址
    std::shared_ptr<NodeResolver> resolver;

    // 发送IO线程数量(Client端)
    size_t send_io_threads = 4;

    // 接收IO线程数量(Server端)
    size_t recv_io_threads = 4;

    Status Validate() const;
};

struct SnapshotOptions {
    // 最多可以有几个快照同时在发送
    uint8_t max_send_concurrency = 5;

    // 最多允许有几个快照同时在应用
    uint8_t max_apply_concurrency = 5;

    // 一次发送多大数据块
    size_t max_size_per_msg = 64 * 1024;

    size_t ack_timeout_seconds = 10;

    // TODO: rate limit

    Status Validate() const;
};

struct RaftServerOptions {
    // 本实例dataserver的节点id
    uint64_t node_id = 0;

    // 是否启用raft pre-vote特性
    bool enable_pre_vote = true;

    // 当learner复制进度追上以后，是否自动提升learner为可投票成员
    bool auto_promote_learner = true;
    // learner跟leader的日志差距小于下面条件中的任意一个就算是进度追上了
    // 1) 条数
    // 2) 当前最大日志index的百分比
    uint64_t promote_gap_threshold = 500;
    uint64_t promote_gap_percent = 5;

    // tick间隔时间
    std::chrono::milliseconds tick_interval = std::chrono::milliseconds(500);

    // 一次心跳是几个tick
    unsigned heartbeat_tick = 1;

    // 选举超时，单位：几个心跳间隔
    unsigned election_tick = 5;

    // 连续几个tick没有收到副本的消息算作副本不活跃，
    unsigned inactive_tick = 10;

    // 每个几个tick，更新一次raft status
    unsigned status_tick = 4;

    // 复制pipeline量（按条数）
    int max_inflight_msgs = 128;

    // 复制batch数量（按字节大小）
    uint64_t max_size_per_msg = 1024 * 1024;

    // raft一致性线程数量
    uint8_t consensus_threads_num = 4;
    // raft一致性队列长度
    size_t consensus_queue_capacity = 100000;

    // 在raft线程里就地apply，不放到apply线程里异步应用
    bool apply_in_place = true;
    // apply线程数量
    uint8_t apply_threads_num = 4;
    // apply队列长度
    size_t apply_queue_capacity = 100000;

    TransportOptions transport_options;
    SnapshotOptions snapshot_options;

    Status Validate() const;
};

struct RaftOptions {
    // raft group id
    uint64_t id = 0;

    // 成员
    std::vector<Peer> peers;

    // 状态机接口
    std::shared_ptr<StateMachine> statemachine;

    // raft日志不持久化，只在内存中存储, 供测试用
    bool use_memory_storage = false;

    // raft日志存储目录
    std::string storage_path;
    // 单个日志文件的大小
    size_t log_file_size = 1024 * 1024 * 16;
    // 最多保留多少个日志文件，超过就截断旧数据
    size_t max_log_files = 5;
    // 启动时检测到日志损坏是否运行继续启动
    bool allow_log_corrupt = false;
    // 日志创建时的起始index
    uint64_t initial_first_index = 0;

    // 已应用的位置，用于raft启动时recover
    uint64_t applied = 0;

    // 指定leader和leader对应的term
    uint64_t leader = 0;
    uint64_t term = 0;

    Status Validate() const;
};

} /* namespace raft */
} /* namespace sharkstore */
