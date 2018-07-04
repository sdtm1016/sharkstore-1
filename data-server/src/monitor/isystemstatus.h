#pragma once

#include "syscommon.h"
#include <memory>
#include <mutex>

namespace sharkstore {
namespace monitor {

class LinuxStatus;
class MacStatus;
class DataCacl;
class EncodeData;
class RangeData;

class ISystemStatus {
public:
    ISystemStatus();

    ~ISystemStatus();

public:
    void GetProcessStats(std::string &str);

public:
    bool GetSysStatus(SysInfo &info);

    bool GetCPUInfo(CpuInfo &info);

    bool GetMemInfo(MemInfo &info);

    bool GetHardDiskInfo(HardDiskInfo &info);

    bool GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available);


public:
    void PutTopData(PrintTag tag, uint32_t time);

    void PutRangeData(RangeTag tag, uint64_t count);

    void PutVersion(const char *version);

    void PutExePath(const char *path);

private:
    void CaclTop();

    void InitPar();

private:
    void GetMemStats(pid_t pid);

    void GetHardDiskStats(pid_t pid);

    void GetDsStats(pid_t pid);

    void GetCpuStats(pid_t pid);

    void GetLoadStats(pid_t pid);

    void GetNetWorkStats(pid_t pid);

    void GetTsStats(pid_t pid);

private:
#ifdef __linux__
    std::shared_ptr<LinuxStatus> statusPtr_ = nullptr;
#elif defined(__APPLE__)
    std::shared_ptr<MacStatus> statusPtr_ = nullptr;
#else
#error unsupport platform
#endif

    std::shared_ptr<DataCacl> cacl_ = nullptr;
    std::shared_ptr<EncodeData> encode_ = nullptr;
    std::shared_ptr<RangeData> rd_ = nullptr;

private:
    std::mutex mutex_;
    char path_[MAX_FILE_NAME] = {0};
    char version_[MAX_FILE_NAME] = {0};
};

}
}
