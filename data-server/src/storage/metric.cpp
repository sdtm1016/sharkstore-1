#include "metric.h"

#include <cassert>
#include <sstream>

namespace sharkstore {
namespace dataserver {
namespace storage {

Metric g_metric;

std::string MetricStat::ToString() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"keys_read_per_sec\": " << keys_read_per_sec << ", ";
    ss << "\"keys_write_per_sec\": " << keys_write_per_sec << ", ";
    ss << "\"bytes_read_per_sec\": " << bytes_read_per_sec << ", ";
    ss << "\"bytes_write_per_sec\": " << bytes_write_per_sec;
    ss << "}";
    return ss.str();
}

Metric::Metric() : last_collect_(std::chrono::steady_clock::now()) {}

Metric::~Metric() {}

void Metric::AddRead(uint64_t keys, uint64_t bytes) {
    keys_read_counter_ += keys;
    bytes_read_counter_ += bytes;
}

void Metric::AddWrite(uint64_t keys, uint64_t bytes) {
    keys_write_counter_ += keys;
    bytes_write_counter_ += bytes;
}

static uint64_t calculateOps(uint64_t val, uint64_t elapsed_ms) {
    return static_cast<uint64_t>(static_cast<double>(val) /
                                 static_cast<double>(elapsed_ms) * 1000);
}

void Metric::Reset() {
    last_collect_ = std::chrono::steady_clock::now();

    keys_read_counter_ = 0;
    keys_write_counter_ = 0;
    bytes_read_counter_ = 0;
    bytes_write_counter_ = 0;
}

void Metric::Collect(MetricStat* stat) {
    assert(stat != nullptr);

    auto now = std::chrono::steady_clock::now();
    auto elasped_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - last_collect_)
                          .count();
    if (elasped_ms <= 0) return;

    stat->keys_read_per_sec =
        calculateOps(keys_read_counter_.exchange(0), elasped_ms);
    stat->keys_write_per_sec =
        calculateOps(keys_write_counter_.exchange(0), elasped_ms);
    stat->bytes_read_per_sec =
        calculateOps(bytes_read_counter_.exchange(0), elasped_ms);
    stat->bytes_write_per_sec =
        calculateOps(bytes_write_counter_.exchange(0), elasped_ms);

    last_collect_ = now;
}

void Metric::CollectAll(MetricStat* stat) { g_metric.Collect(stat); }

}  // namespace storage
}  // namespace dataserver
}  // namespace sharkstore
