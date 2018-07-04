#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

#include <gperftools/profiler.h>

#include "address.h"
#include "config.h"
#include "node.h"
#include "raft/src/impl/logger.h"

using namespace sharkstore;
using namespace sharkstore::raft;
using namespace sharkstore::raft::bench;

struct BenchContext {
    std::atomic<int64_t> counter;
    std::vector<std::shared_ptr<Range>> leaders;
};

void runBenchmark(BenchContext *ctx) {
    while (true) {
        std::vector<std::shared_future<bool>> futures;
        for (size_t i = 0; i < bench_config.concurrency; ++i) {
            auto num = ctx->counter.fetch_sub(1);
            if (num > 0) {
                futures.push_back(
                    (ctx->leaders)[num % ctx->leaders.size()]->AsyncRequest());
            } else {
                break;
            }
        }
        if (!futures.empty()) {
            for (auto &f : futures) {
                f.wait();
                assert(f.valid());
                assert(f.get());
            }
        } else {
            return;
        }
    }
}

namespace sharkstore {
namespace raft {
namespace bench {
void start_fast_service(int argc, char *argv[]);
}
}  // namespace raft
}  // namespace sharkstore

int main(int argc, char *argv[]) {
    // 不打印debug日志
    SetLogger(new impl::StdLogger(impl::StdLogger::Level::kInfo));

    start_fast_service(argc, argv);

    auto addr_mgr = std::make_shared<bench::NodeAddress>(3);

    std::vector<std::shared_ptr<bench::Node>> cluster;
    for (size_t i = 1; i <= 3; ++i) {
        auto node = std::make_shared<bench::Node>(i, addr_mgr);
        node->Start();
        cluster.push_back(node);
    }

    BenchContext context;
    context.counter = bench_config.request_num;
    context.leaders.resize(bench_config.range_num);
    for (uint64_t i = 1; i <= bench_config.range_num; ++i) {
        for (auto &n : cluster) {
            auto r = n->GetRange(i);
            r->WaitLeader();
            if (r->IsLeader()) {
                context.leaders[i] = r;
            }
        }
    }

    // 开始
    ProfilerStart("./bench.prof");
    struct timeval start, end, taken;
    gettimeofday(&start, NULL);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < bench_config.thread_num; ++i) {
        threads.push_back(std::thread(std::bind(&runBenchmark, &context)));
    }
    for (auto &t : threads) {
        t.join();
    }

    // 结束
    gettimeofday(&end, NULL);
    timersub(&end, &start, &taken);
    ProfilerStop();

    std::cout << bench_config.request_num << " requests taken " << taken.tv_sec << "s "
              << taken.tv_usec / 1000 << "ms" << std::endl;

    std::cout << "ops: "
              << (bench_config.request_num * 1000) /
                     (taken.tv_sec * 1000 + taken.tv_usec / 1000)
              << std::endl;

    return 0;
}