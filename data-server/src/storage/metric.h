_Pragma("once");

#include <atomic>
#include <chrono>
#include <string>

namespace sharkstore {
namespace dataserver {
namespace storage {

struct MetricStat {
    uint64_t keys_read_per_sec = 0;
    uint64_t keys_write_per_sec = 0;
    uint64_t bytes_read_per_sec = 0;
    uint64_t bytes_write_per_sec = 0;

    std::string ToString() const;
};

class Metric {
public:
    Metric();
    ~Metric();

    void AddRead(uint64_t keys, uint64_t bytes);
    void AddWrite(uint64_t keys, uint64_t bytes);

    void Reset();

    // should only called by one thread
    void Collect(MetricStat* stat);

    // collect gobal metric
    static void CollectAll(MetricStat* stat);

private:
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    std::atomic<uint64_t> keys_read_counter_{0};
    std::atomic<uint64_t> keys_write_counter_{0};
    std::atomic<uint64_t> bytes_read_counter_{0};
    std::atomic<uint64_t> bytes_write_counter_{0};

    TimePoint last_collect_;
};

extern Metric g_metric;

}  // namespace storage
}  // namespace dataserver
}  // namespace sharkstore
