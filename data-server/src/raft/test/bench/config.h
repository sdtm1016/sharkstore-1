_Pragma("once");

#include <cstddef>

namespace sharkstore {
namespace raft {
namespace bench {

struct BenchConfig {
    std::size_t request_num = 1000000;
    std::size_t thread_num = 3;
    std::size_t range_num = 1;
    std::size_t concurrency = 100;

    bool use_memory_raft_log = false;
    bool use_inprocess_transport = false;
    std::size_t raft_thread_num = 1;
    std::size_t apply_thread_num = 1;
};

extern BenchConfig bench_config;

} /* namespace bench */
} /* namespace raft */
} /* namespace sharkstore */
