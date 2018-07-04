#pragma once

#include <stdint.h>
#include <mutex>
#include <string>

namespace sharkstore
{
    namespace monitor
    {
        #define VMRSS_LINE     15           //VMRSS所在行
        #define PROCESS_ITEM  14          //进程CPU时间开始的项数
        #define MEMINFO_LINE  15          //meminfo文件的控制行数

        #define READ_CHARS_LENGTH    256

        #define PROC_NET_DEV_FNAME "/proc/net/dev"

        #define DELAY_TIME  500000         //延时时间，计算CPU

        #define ARRAY_LEN  100000          //计算TOP的数组

        #define CACL_ARRAY_LEN  8

        #define MAX_FILE_NAME 256

        typedef unsigned long long ULong64 ;

        const std::string TagName[CACL_ARRAY_LEN] = {"Net","Store","Task","Qwait","Raft","Get","Set","Deal"};
        const double TIME_UNIT = 0.001 * 0.001;
        const int    DISP_UNIT = 1000 ;

        enum class PrintTag
        {
            Net,Store,Task,Qwait,Raft,Get,Set,Deal
        };

        enum class RangeTag
        {
            RangeCount, SplitCount, SendSnapCount, RecvSnapCount, ApplySnapCount, LeaderCount
        };

        typedef struct __CaclResult
        {
            PrintTag Tag;
            double Min = 0.0;
            double Max = 0.0;
            double Avg = 0.0;

            double Top50 = 0.0;
            double Top90 = 0.0;
            double Top99 = 0.0;
            double Top999 = 0.0;
            uint32_t Len = 0;
        }CaclResult,*PCaclResult;

        typedef struct __CaclValue
        {
            uint32_t FirstBuf[ARRAY_LEN] = {0};
            uint32_t SecondBuf[ARRAY_LEN] = {0};
            uint32_t *PCur= 0;
            uint32_t *PUseCacl = 0;

            uint64_t Num = 0;
            uint64_t Swap = 0;
            PrintTag Tag;

            std::mutex Mut;
        }CaclValue,*PCaclValue;

        typedef struct   __TotalCpuT
        {
            uint32_t User;                           //从系统启动开始累计到当前时刻，处于用户态的运行时间，不包含 nice值为负进程。
            uint32_t Nice;                           //从系统启动开始累计到当前时刻，nice值为负的进程所占用的CPU时间
            uint32_t System;                      //从系统启动开始累计到当前时刻，处于核心态的运行时间
            uint32_t Idle;                           //从系统启动开始累计到当前时刻，除IO等待时间以外的其它等待时间iowait (12256) 从系统启动开始累计到当前时刻，IO等待时间(since 2.5.41)
        }TotalCpuT,*PTotalCpuT;

        typedef struct __ProcessCpuT
        {
            int Pid;                                        //pid号
            uint32_t Utime;                            //该任务在用户态运行的时间，单位为 jiffies或HZ
            uint32_t Stime;                            //该任务在核心态运行的时间，单位为jiffies
            uint32_t Cutime;                          //所有已死线程在用户态运行的时间，单位为jiffies
            uint32_t Cstime;                          //所有已死在核心态运行的时间，单位为jiffies
        }ProcessCpuT,*PProcessCpuT;

        enum class UnitType
        {
            B,KB,MB,GB,TB,PB
        };
        typedef struct  __MemInfo
        {
            //UnitType Unit;
            uint64_t Total;
            uint64_t Used;
            uint64_t Free;
            uint64_t UsedRss;//进程级别，只有此项有意义
            double Rate;

            //新增
            uint32_t ThreadCount;
            uint64_t RssUsed;
            uint64_t SwapTotal;
            uint64_t SwapUsed;
            uint64_t SwapFree;
            double SwapRate;
        }MemInfo,*PMemInfo;
        typedef struct __DiskRWStatus
        {
            uint64_t Count;
            uint64_t TimeStamp;

            uint64_t CurReadBytesSec;
            uint64_t CurWriteBytesSec;
            uint64_t CurReadCountSec;
            uint64_t CurWriteCountSec;
        }DiskRwStatus,*PDiskRwStatus;
        typedef struct __HardDiskInfo
        {
        // UnitType Unit;
            char Name[256];
            uint64_t Total;
            uint64_t Used;
            uint64_t Free;
            double Rate;

        //硬盘每秒读写的BYTE数量及读写次数
            uint64_t ReadBytesSec;
            uint64_t WriteBytesSec;
            uint64_t ReadCountSec;
            uint64_t WriteCountSec;
        }HardDiskInfo,*PHardDiskInfo;
        typedef struct __CpuInfo
        {
            uint64_t Used;
            uint32_t CpuCount;
            double Rate;
        }CpuInfo,*PCpuInfo;

        typedef struct __SysInfo
        {
            CpuInfo  Cpu;
            MemInfo Mem;
            HardDiskInfo HardDisk;
        }SysInfo,*PSysInfo;

        //network info
        typedef struct __NetdevStats
        {
            unsigned long long RxPacketsM;    //接收到的全部包数量
            unsigned long long TxPacketsM;   // 发送的全部包
            unsigned long long RxBytesM;     // 接收的总的字节数
            unsigned long long TxBytesM;     // 发送的总的字节数
            unsigned long RxErrorsM;            // 接收到的错误包

            unsigned long TxErrorsM;            // 发送的错误包
            unsigned long RxDroppedM;         // 接收丢弃的包
            unsigned long TxDroppedM;         //发送丢弃的包
            unsigned long RxMulticastM;         //接收的广播包
            unsigned long RxCompressedM;   //接收的压缩包

            unsigned long TxCompressedM; //发送的压缩包
            unsigned long CollisionsM;         //冲突包

            // 接收错误
            unsigned long RxLengthErrorsM;
            unsigned long RxOverErrorsM;         //receivEr ring buff ovErflow
            unsigned long RxCrcErrorsM;            // recved pkt with crc Error
            unsigned long RxFrameErrorsM;        // recv'd frame alignment Error
            unsigned long RxFifoErrorsM;          // recv'r fifo ovErrun
            unsigned long RxMissedErrorsM;       // receivEr missed packet

            // 发送错误
            unsigned long TxAbortedErrorsM;
            unsigned long TxCarrierErrorsM;
            unsigned long TxFifoErrorsM;
            unsigned long TxHeartbeatErrorsM;
            unsigned long TxWindowErrorsM;
        }NetdevStats,*PNetdevStats;

        typedef struct __ClusterStats
        {
            uint64_t CapacityTotal;
            uint64_t UsedSize;
            uint64_t RangeNum;
            uint64_t DbNum;
            uint64_t TableNum;

            uint64_t TaskNum;
            uint64_t DsNum;
        }ClusterStats,*PClusterStats;

        typedef struct __TpStats
        {
            uint64_t Tps;
            double Min;
            double Max;
            double Avg;
            double Tp50;

            double Tp90;
            double Tp99;
            double Tp999;
        }TpStats,*PTpStats;

        typedef struct __DsInfo
        {
            uint64_t RangeCount;
            uint32_t RangeSplitCount;
            uint32_t SwndingSnapCount;
            uint32_t ReceivingSnapCount;
            uint32_t ApplyingSnapCount;

            uint32_t RangeLeaderCount;
            char Ver[256];
        }DsInfo,*PDsInfo;
        typedef struct __LoadAvg
        {
            double Load1;
            double Load5;
            double Load15;
        }LoadAvg,*PLoadAvg;

        typedef struct __ProcessStats
        {
            double CpuRate;
            uint32_t CpuCount;
            double Load1;
            double Load5;
            double Load15;

            //CpuInfo  Cpu;
            MemInfo Mem;
            NetdevStats NetStats;
            HardDiskInfo HardDisk;
            TpStats Tp;
            DsInfo Ds;

            uint32_t ThreadNum;
            uint32_t HandleNum;
            uint64_t StartTime;

        }ProcessStats,*PProcessStats;
    }
}
