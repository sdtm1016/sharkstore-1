#include "mac_status.h"

#include <sys/param.h>
#include <sys/mount.h>

namespace sharkstore {
namespace monitor {

bool MacStatus::GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) {
    struct statfs buf;
    memset(&buf, 0, sizeof(buf));
    int ret = ::statfs(path, &buf);
    if (ret == 0) {
        *total = buf.f_bsize * buf.f_blocks;
        *available = buf.f_bsize * buf.f_bavail;
        return true;
    } else {
        return false;
    }
}

}
}
