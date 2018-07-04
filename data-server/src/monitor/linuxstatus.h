#pragma once
#include "syscommon.h"
#include <string>
#include <vector>
#include <sys/types.h>
namespace sharkstore {
namespace monitor {

class LinuxStatus {
public:
    LinuxStatus();
    ~LinuxStatus();
public:
    bool GetSysStatus(SysInfo&info);
    bool GetCPUInfo(CpuInfo&info);
    bool GetMemInfo(MemInfo&info);
    bool GetHardDiskInfo(HardDiskInfo&info);

    bool GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available);

    bool GetMemProcInfo(MemInfo &info,const pid_t  id);
    bool GetCPUInfo(CpuInfo &info,const pid_t  id);
public:
    bool GetPathFromDisk(const char *path,std::string &name);
private:
    bool GetLinuxMem(MemInfo &info);
    bool GetDiskSize(std::vector<HardDiskInfo> & vecSize, HardDiskInfo&info);
    bool GetDiskBlockSize(std::vector<HardDiskInfo> & vecSize);
    void DiskInfoUinque(std::vector<HardDiskInfo> & vecSize);

    void LinesFrom(FILE *pFile, std::vector<std::string> &vResult);
    int ExecCmd(const char *szCmd, std::vector<std::string> &vResult);
    int CPUCurCalc(std::vector<uint64_t> &vltTotal, std::vector<uint64_t> &vltIdle);

    const char *GetFiled(const char *pData,int pos);
    uint64_t GetMemUse(const pid_t pid,uint32_t &count);
    uint64_t GetTotalMem(MemInfo &info);

    uint32_t  GetProcCPU(const pid_t pid);
    uint32_t  GetTotalCPU();

    char * GetName(char * name, char * p);
    uint32_t GetCpuNum();
private:
    bool GetRWStatus(HardDiskInfo &hdi);
    uint64_t GetTimeStamp();
public:
    int GetLoadAvg(LoadAvg&lv);
    int GetNetWorkStats(char * ifname,  NetdevStats * pstats);
    uint32_t GetFileCount(pid_t pid);

private:
    std::vector<HardDiskInfo> vecSize_;
    DiskRwStatus drs_;
};

}
}