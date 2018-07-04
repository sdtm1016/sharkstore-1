#pragma once

#include "syscommon.h"
#include <string>
#include <vector>
#include <sys/types.h>

namespace sharkstore {
namespace monitor {

class MacStatus {
public:
    MacStatus() = default;
    ~MacStatus() = default;

public:
    bool GetSysStatus(SysInfo&info) { return false; }
    bool GetCPUInfo(CpuInfo&info) { return false; }
    bool GetMemInfo(MemInfo&info) { return false; }
    bool GetHardDiskInfo(HardDiskInfo&info) { return false; }

    bool GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available);

    bool GetMemProcInfo(MemInfo &info,const pid_t  id) { return false; }
    bool GetCPUInfo(CpuInfo &info,const pid_t  id) { return false; }
    bool GetPathFromDisk(const char *path,std::string &name) { return false; }
    int GetLoadAvg(LoadAvg&lv) { return 0; }
    int GetNetWorkStats(char * ifname,  NetdevStats * pstats) { return 0; }
    uint32_t GetFileCount(pid_t pid) { return 0; }
};

}
}
