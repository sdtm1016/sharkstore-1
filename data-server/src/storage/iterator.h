_Pragma("once");

#include <rocksdb/db.h>
#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Metric;

class Iterator {
public:
    Iterator(rocksdb::Iterator* it, const std::string& start,
             const std::string& limit);
    ~Iterator();

    bool Valid();
    void Next();

    Status status();
    std::string key();
    std::string value();

    uint64_t key_size();
    uint64_t value_size();

private:
    rocksdb::Iterator* rit_ = nullptr;
    const std::string limit_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
