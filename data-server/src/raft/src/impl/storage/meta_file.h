_Pragma("once");

#include <stdint.h>
#include "base/status.h"

#include "../raft.pb.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

class MetaFile {
public:
    explicit MetaFile(const std::string& path);
    ~MetaFile();

    MetaFile(const MetaFile&) = delete;
    MetaFile& operator=(const MetaFile&) = delete;

    Status Open();
    Status Close();
    Status Sync();
    Status Destroy();

    Status Load(pb::HardState* hs, pb::TruncateMeta* tm);
    Status SaveHardState(const pb::HardState& hs);
    Status SaveTruncMeta(const pb::TruncateMeta& tm);

private:
    constexpr static size_t kHardStateSize = 8 * 3;     // term(8) + commit(8) + vote(8)
    constexpr static size_t kTruncateMetaSize = 8 * 2;  // index(8) + term(8)

private:
    const std::string path_;
    int fd_ = -1;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
