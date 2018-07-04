#include "isystemstatus.h"

#include <string.h>
#include <unistd.h>
#include <time.h>

#include <functional>
#include <algorithm>
#include <math.h>

#include "linuxstatus.h"
#include "mac_status.h"
#include "datacacl.h"
#include "encodedata.h"
#include "rangedata.h"

namespace sharkstore
{
    namespace monitor
    {
        ProcessStats g_pStats;
        ProcessStats g_pStatsOld;
        time_t g_oldTime;
        time_t starttime = 0;
        bool g_once = true;

        ISystemStatus::ISystemStatus()
        {
            if (nullptr == statusPtr_)
            {
#ifdef __linux__
                this->statusPtr_ = std::make_shared<LinuxStatus>();
#elif defined(__APPLE__)
                this->statusPtr_ = std::make_shared<MacStatus>();
#else
#error unsupport platform
#endif
            }

            if (nullptr == cacl_)
            {
                this->cacl_ = std::make_shared<DataCacl>();
            }

            if (nullptr == rd_)
            {
                this->rd_ = std::make_shared<RangeData>();
            }

            if (nullptr == encode_)
            {
                this->encode_ = std::make_shared<EncodeData>();
            }

        }
        ISystemStatus::~ISystemStatus()
        {

        }
        void ISystemStatus::InitPar()
        {
            memset(&g_pStats,0,sizeof(g_pStats));

            MemInfo mInfo;
	        memset(&mInfo,0,sizeof(mInfo));
            CpuInfo cpuInfo;
	        memset(&cpuInfo,0,sizeof(cpuInfo));
            LoadAvg lAvg;
	        memset(&lAvg,0,sizeof(lAvg));
            NetdevStats netStats;
	        memset(&netStats,0,sizeof(netStats));
            HardDiskInfo hdInfo;
	        memset(&hdInfo,0,sizeof(hdInfo));
        }
        bool ISystemStatus::GetSysStatus(SysInfo&info)
        {
            bool bRet = false;

            return bRet;
        }
        bool ISystemStatus::GetCPUInfo(CpuInfo&info)
        {
            bool bRet = false;

            return bRet;
        }
        bool ISystemStatus::GetMemInfo(MemInfo&info)
        {
            bool bRet = false;
            bRet = this->statusPtr_->GetMemInfo(info);

            return bRet;
        }
        bool ISystemStatus::GetHardDiskInfo(HardDiskInfo&info)
        {
            bool bRet = false;
            return this->statusPtr_->GetHardDiskInfo(info);
            return bRet;
        }

        bool ISystemStatus::GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) {
            return this->statusPtr_->GetFileSystemUsage(path, total, available);
        }

        //拆分GetProcessStats函数
        void ISystemStatus::GetMemStats(pid_t pid)
        {
            MemInfo mInfo;
	        memset(&mInfo,0,sizeof(mInfo));

            this->statusPtr_->GetMemProcInfo(mInfo,pid);
            g_pStats.ThreadNum = mInfo.ThreadCount;
            g_pStats.Mem = mInfo;
        }
        void ISystemStatus::GetHardDiskStats(pid_t pid)
        {
            HardDiskInfo hdInfo;
	        memset(&hdInfo,0,sizeof(hdInfo));

            this->statusPtr_->GetHardDiskInfo(hdInfo);
            g_pStats.HardDisk.Free = hdInfo.Free;
            g_pStats.HardDisk.Rate = hdInfo.Rate;
            g_pStats.HardDisk.Total = hdInfo.Total;
            g_pStats.HardDisk.Used = hdInfo.Used;
            g_pStats.HardDisk.ReadBytesSec = hdInfo.ReadBytesSec;
            g_pStats.HardDisk.ReadCountSec = hdInfo.ReadCountSec;
            g_pStats.HardDisk.WriteBytesSec = hdInfo.WriteBytesSec;
            g_pStats.HardDisk.WriteCountSec = hdInfo.WriteCountSec;
        }





        void ISystemStatus::GetDsStats(pid_t pid)
        {
            //get ds info
            DsInfo ds;
            this->rd_->GetResult(ds);
            g_pStats.Ds.ApplyingSnapCount = ds.ApplyingSnapCount;
            g_pStats.Ds.RangeCount = ds.RangeCount;
            g_pStats.Ds.RangeLeaderCount = ds.RangeLeaderCount;
            g_pStats.Ds.RangeSplitCount = ds.RangeSplitCount;
            g_pStats.Ds.ReceivingSnapCount = ds.ReceivingSnapCount;
            g_pStats.Ds.SwndingSnapCount = ds.SwndingSnapCount;
            strcpy(g_pStats.Ds.Ver,this->version_);
        }
        void ISystemStatus::GetCpuStats(pid_t pid)
        {
            CpuInfo cpuInfo;
	        memset(&cpuInfo,0,sizeof(cpuInfo));

            this->statusPtr_->GetCPUInfo(cpuInfo,pid);
            g_pStats.CpuCount = cpuInfo.CpuCount;
            g_pStats.CpuRate = cpuInfo.Rate;
        }
        void ISystemStatus::GetLoadStats(pid_t pid)
        {
            LoadAvg lAvg;
	        memset(&lAvg,0,sizeof(lAvg));

            this->statusPtr_->GetLoadAvg(lAvg);
            g_pStats.Load1 = lAvg.Load1;
            g_pStats.Load5 = lAvg.Load5;
            g_pStats.Load15 = lAvg.Load15;
        }
        void ISystemStatus::GetNetWorkStats(pid_t pid)
        {
            NetdevStats netStats;
	        memset(&netStats,0,sizeof(netStats));
            std::string netName = "";

            //匹配硬盘的信息名称和网卡的名称未给定，使用默认
            this->statusPtr_->GetNetWorkStats(NULL,&netStats);
        }
        void ISystemStatus::GetTsStats(pid_t pid)
        {
            CaclResult result[CACL_ARRAY_LEN];
            this->cacl_->GetResult(result);

            for (int len = 0;len < CACL_ARRAY_LEN;len++)
            {
                if (result[len].Tag == PrintTag::Deal)
                {
                    g_pStats.Tp.Avg = result[len].Avg;
                    g_pStats.Tp.Max = result[len].Max;
                    g_pStats.Tp.Min = result[len].Min;
                    g_pStats.Tp.Tp50 = result[len].Top50;
                    g_pStats.Tp.Tp90 = result[len].Top90;
                    g_pStats.Tp.Tp99 = result[len].Top99;
                    g_pStats.Tp.Tp999 = result[len].Top999;

                    break;
                }
            }
        }
        void ISystemStatus::GetProcessStats(std::string & str)
        {
            memset(&g_pStats,0,sizeof(g_pStats));
            pid_t pid = getpid();

            time(&starttime);
            uint32_t filecount = 0;

            this->GetCpuStats(pid);
            this->GetMemStats(pid);
            this->GetHardDiskStats(pid);
            this->GetLoadStats(pid);
            this->GetNetWorkStats(pid);
            this->GetDsStats(pid);
            this->GetTsStats(pid);

            filecount = this->statusPtr_->GetFileCount(pid);
            g_pStats.HandleNum = filecount;

            std::string name = "";
            this->statusPtr_->GetPathFromDisk(this->path_,name);
            strcpy(g_pStats.HardDisk.Name,name.c_str());

            if (g_once)
            {
                g_pStats.StartTime = starttime;
                g_once = false;
            }

            this->encode_->SysInfoToJson(g_pStats,str);

            g_oldTime = starttime;
            g_pStatsOld = g_pStats;

            return ;
        }

        void ISystemStatus::PutTopData(PrintTag tag,uint32_t timedata)
        {
            cacl_->PutData(tag,timedata);
        }

        void ISystemStatus::PutRangeData(RangeTag tag, uint64_t count)
        {
            rd_->PutData(tag,count);
        }

        void ISystemStatus::PutVersion(const char *version)
        {
            if (NULL != version)
            {
                strcpy(this->version_,version);
            }

        }
        void ISystemStatus::PutExePath(const char * path)
        {
            if (NULL != path)
            {
                strcpy(this->path_,path);
            }
        }
    }
}
