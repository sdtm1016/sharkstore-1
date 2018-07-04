#include "linuxstatus.h"
#include <sys/sysinfo.h>
#include <sys/vfs.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <mntent.h>
#include <string.h>

#include <iostream>
#include <stdio.h>
#include <cstring>
#include <sys/times.h>
#include <sys/time.h>
#include <sys/vtimes.h>
#include <sys/socket.h> 

#include <stdlib.h>
#include <dirent.h>
#include <fstream>
#include <unistd.h> 
#include <netdb.h>  
#include <arpa/inet.h> 
#include <map>
#include <inttypes.h>

namespace sharkstore
{
    namespace monitor
    {
        LinuxStatus::LinuxStatus()
        {
            memset(&drs_,0,sizeof(drs_));
        }
        LinuxStatus::~LinuxStatus()
        {

        }
        bool LinuxStatus::GetSysStatus(SysInfo&info)
        {
            bool bRet = true;
            this->GetCPUInfo(info.Cpu);
            this->GetMemInfo(info.Mem);
            this->GetHardDiskInfo(info.HardDisk);
            return bRet;
        }
        bool LinuxStatus::GetCPUInfo(CpuInfo&info)
        {
            bool bRet = true;
            std::vector<uint64_t> vltTotalFirst, vltIdleFirst,vUtilization;
            int num = CPUCurCalc(vltTotalFirst,vltIdleFirst);
            sleep(1); 

            std::vector<uint64_t> vltTotalSecond, vltIdleSecond;
            int nCores = CPUCurCalc(vltTotalSecond, vltIdleSecond);
            for(int i = 0; i < nCores; i++)
            {
                double divisor = (vltIdleSecond[i] - vltIdleFirst[i]) * 1.0;
                double dividend = (vltTotalSecond[i] - vltTotalFirst[i]) * 1.0;

                info.Rate  = 0.0;
                if (dividend <= 0 || divisor <= 0)
                {
                    info.Rate = 0.0;
                    return false;
                }

                double dvalue = dividend - divisor;
                if (dvalue <= 0)
                {
                    info.Rate = 0;
                    return false;
                }

                info.Rate = dvalue/dividend;
            } 

            return bRet;
        }
        bool LinuxStatus::GetMemInfo(MemInfo&info)
        {
        return  this->GetLinuxMem(info);
        }
        bool LinuxStatus::GetHardDiskInfo(HardDiskInfo&info)
        {
            bool bRet = true;
            if (!this->GetDiskSize(vecSize_,info))
            {
                bRet = false;
            }

            if (!this->GetRWStatus(info))
            {
                bRet = false;
            }

            return bRet;
        }

        bool LinuxStatus::GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) {
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

        bool LinuxStatus::GetLinuxMem(MemInfo &info)
        {
            bool bRet = true;
            struct sysinfo si;
            int err = sysinfo(&si);

            if (-1 == err)
            {
                bRet = false;
            }

            //do with meminfo
            double unit = 100.00;
            info.Total = si.totalram;
            info.Free = si.freeram;
            info.Used = si.totalram - si.freeram;
            info.Rate = info.Used *unit /info.Total;

            return bRet;
        }
        bool LinuxStatus::GetDiskSize(std::vector<HardDiskInfo> & vecSize,HardDiskInfo &hdi)
        {
            bool bRet = true;
            vecSize.clear();
        
            this->DiskInfoUinque(vecSize);
            //得到总大小
            strcpy(hdi.Name , "all");
            for (uint32_t num  = 0;num < vecSize.size();num++)
            {
                hdi.Total += vecSize[num].Total;
                hdi.Used += vecSize[num].Used;
                hdi.Free += vecSize[num].Free;
            }

            double unit = 100.00;
            hdi.Rate = unit *hdi.Used /hdi.Total;

            return bRet;
        }
        bool LinuxStatus::GetDiskBlockSize(std::vector<HardDiskInfo> & vecSize)
        {
                FILE* mountTable = NULL;
                struct mntent *mountEntry = NULL;  
                struct statfs s;  
                unsigned long blocksUsed = 0;  
                unsigned blocksPercentUsed = 0;

                mountTable = setmntent("/etc/mtab", "r");  
                if (!mountTable)  
                {  
                    return false;  
                }  

                while (true) 
                {  
                    const char *device= NULL;  
                    const char *mountPoint=NULL;  

                    HardDiskInfo hdi;
                    memset(&hdi,0,sizeof(hdi));

                    if (mountTable) 
                    {  
                        mountEntry = getmntent(mountTable);  
                        if (!mountEntry) 
                        {  
                            endmntent(mountTable);  
                            break;  
                        }  
                    }   
                    else  
                    {
                        continue;
                    }
                        
                    device = mountEntry->mnt_fsname;  
                    mountPoint = mountEntry->mnt_dir;  

                    int err = statfs(mountPoint, &s);
                    if ( err != 0)   
                    {  //ENOENT
                        if (ENOENT != err)
                        {
                            fprintf(stderr, "statfs failed!\n");  
                        }       
                        continue;  
                    }

                    if ((s.f_blocks > 0) || !mountTable )   
                    {  
                        blocksUsed = s.f_blocks - s.f_bfree;  
                        blocksPercentUsed = 0;  
                        if (blocksUsed + s.f_bavail)   
                        {  
                            blocksPercentUsed = (blocksUsed * 100ULL  
                                    + (blocksUsed + s.f_bavail)/2  
                                    ) / (blocksUsed + s.f_bavail);  
                        }  
        
                        if (strcmp(device, "rootfs") == 0)  
                        {
                            continue;
                        }

                        //save size
                        hdi.Total = s.f_blocks * s.f_bsize;
                        hdi.Used = (s.f_blocks-s.f_bfree)*s.f_bsize;     
                        hdi.Free = s.f_bavail*s.f_bsize;
                        hdi.Rate = blocksPercentUsed;
                        strcpy(hdi.Name,device) ;    

                        vecSize.emplace_back(hdi);   
                    }  
                }  

                return 0; 
        }
        void LinuxStatus::DiskInfoUinque(std::vector<HardDiskInfo> & vecSize)
        {
            std::vector<HardDiskInfo> vec;
            this->GetDiskBlockSize(vec);

            if (vec.size() > 0)
            {
                //去重
                for(uint32_t num = 0;num < vec.size();num++)
                {
                    if (vecSize.size() > 0)
                    {
                        bool exist = false;
                        for (uint32_t len = 0;len < vecSize.size();len++)
                        {
                            if (!strcmp(vec[num].Name,vecSize[len].Name))
                            {
                                exist = true;
                                break;
                                //continue;
                            }                          
                        }

                        if (!exist)
                        {
                            vecSize.emplace_back(vec[num]);
                        }

                    }
                    else
                    {
                        vecSize.emplace_back(vec[num]);
                    }
                }
            }
        }

        void LinuxStatus::LinesFrom(FILE *pFile, std::vector<std::string> &vResult)
        {
            vResult.clear();
            char szLine[READ_CHARS_LENGTH] = { 0 };
            while(fgets(szLine, READ_CHARS_LENGTH, pFile) != NULL)
            {
                int nLength = strlen(szLine);
                if(szLine[nLength - 1] == '\n')
                {
                    szLine[nLength - 1] = '\0';
                }
                
                vResult.push_back(szLine);
            }
        }

        int LinuxStatus::ExecCmd(const char *szCmd, std::vector<std::string> &vResult)
        {    
            FILE *pCmd = popen(szCmd, "r");
            if(pCmd == NULL) 
            {
                    return -1;
            } 
            LinesFrom(pCmd, vResult);

            return pclose(pCmd);
        }

        int LinuxStatus::CPUCurCalc(std::vector<uint64_t> &vltTotal, std::vector<uint64_t> &vltIdle)
        {
            return 32;
            vltTotal.clear();
            vltIdle.clear();    
            // /proc/stat | grep cpu | awk -F ' ' '{ print $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9, $5 }'
            std::vector<std::string> vResult;
            ExecCmd("cat /proc/stat | grep cpu | awk -F ' ' '{ print $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9, $5 }'", vResult);
                
            int nCores = vResult.size();
            for(int i = 0; i < nCores; i++)
            {
                uint64_t ltTotal, ltIdle;
                sscanf(vResult[i].c_str(), "%ld %ld ", &ltTotal, &ltIdle);
                vltTotal.push_back(ltTotal);
                vltIdle.push_back(ltIdle);
            }

            return nCores;
        }
        bool LinuxStatus::GetMemProcInfo(MemInfo &info,const pid_t pid)
        {
            bool bRet = true;
            uint64_t used = GetMemUse(pid,info.ThreadCount);
            uint64_t total = GetTotalMem(info);
            
            info.UsedRss = used;

            float occupy = (used * 1.0)/(total * 1.0);
            info.Used = info.Total - info.Free;
            info.SwapUsed = info.SwapTotal - info.SwapFree;
            info.Rate = info.UsedRss * 100.0/info.Total;
            info.SwapRate = info.SwapUsed*100.0/info.SwapTotal;

            return bRet;
        }
        bool LinuxStatus::GetCPUInfo(CpuInfo &info,const pid_t  id)
        {
            bool bRet = true;
            unsigned int total1,total2;
            unsigned int proc1,proc2;
            total1 = this->GetTotalCPU();
            proc1 = this->GetProcCPU(id);
            usleep(DELAY_TIME);//延迟500毫秒
            total2 = this->GetTotalCPU();
            proc2 = this->GetProcCPU(id);
            float pcpu = 100.0*(proc2 - proc1)/(total2 - total1);

            info.Rate = pcpu;
            info.Used = proc2 - proc1;
            info.CpuCount = this->GetCpuNum();
            
            return bRet;
        }
        uint64_t LinuxStatus::GetMemUse(const pid_t pid,uint32_t &threadCount)
        {
            return 0;
            char file[64] = {0};
        
            FILE *fd = NULL;         
            char lineBuff[MAX_FILE_NAME] = {0};  
            sprintf(file,"/proc/%d/status",pid);//第11行
                                                                                                        
            fd = fopen (file, "r"); 
            if (NULL == fd)
            {
                return 0;
            }

            //vmrss:实际内存占用
            int i = 0;
            char name[32];
            uint64_t vmrss = 0;//内存峰值大小
            while (true)
            {
                char * end = fgets (lineBuff, sizeof(lineBuff), fd);
                if (NULL == end)
                {
                    break;
                }

		        sscanf(lineBuff,"%s",name);

                if (!strcmp(name,"VmRSS:"))
                {
                    sscanf(lineBuff,"%s %lu",name,&vmrss);
                }
                if (!strcmp(name,"Threads:"))
                {
                    sscanf(lineBuff,"%s %u",name,&threadCount);
                    if (threadCount < 1)
                    {
                        threadCount = 1;
                    }
                }  
            }


            fclose(fd);     

            return vmrss * 1024;
        }

        uint64_t LinuxStatus::GetTotalMem(MemInfo &info)
        {
            const char* file = "/proc/meminfo";
        
            FILE *fd = NULL;       
            char lineBuff[256] = {0};                                                                                                
            fd = fopen (file, "r"); 
            if (NULL == fd)
            {
                return 0;
            }

            int i = 0;
            char name[32];
            uint64_t total = 0;
            uint64_t buffer = 0;
            uint64_t cached = 0;

            char * end = fgets (lineBuff, sizeof(lineBuff), fd);//memtotal在第1行
            if (NULL == end)
            {
                return 0;
            }
            sscanf (lineBuff, "%s %lu", name,&total);
            info.Total = total * 1024;

            end = fgets (lineBuff, sizeof(lineBuff), fd);//free在第2行
            if (NULL == end)
            {
                return 0;
            }
            sscanf (lineBuff, "%s %lu", name,&info.Free);

            end = fgets (lineBuff, sizeof(lineBuff), fd);//MemAvailable在第3行--ignore
            if (NULL == end)
            {
                return 0;
            }

            end = fgets (lineBuff, sizeof(lineBuff), fd);//Buffer在第4行
            if (NULL == end)
            {
                return 0;
            }
            sscanf (lineBuff, "%s %lu", name,&buffer);

            end = fgets (lineBuff, sizeof(lineBuff), fd);//Cached在第5行
            if (NULL == end)
            {
                return 0;
            }
            sscanf (lineBuff, "%s %lu", name,&cached);
            info.Free = (info.Free + buffer + cached)* 1024;   //free= memfree+buffer+cached

            //swap在第15，16行
            for(int num = 0;num < MEMINFO_LINE - 6;num++ )
            {
                end = fgets (lineBuff, sizeof(lineBuff), fd);
                if (NULL == end)
                {
                    return 0;
                }
            }

            end = fgets (lineBuff, sizeof(lineBuff), fd);//swaptotal在第15行
            if (NULL == end)
            {
                return 0;
            }
            sscanf (lineBuff, "%s %lu", name,&info.SwapTotal);
            info.SwapTotal = info.SwapTotal * 1024;
            end = fgets (lineBuff, sizeof(lineBuff), fd);//swapfree在第16行
            sscanf (lineBuff, "%s %lu", name,&info.SwapFree);
            info.SwapFree = info.SwapFree * 1024;

            fclose(fd);    

            return total * 1024;
        }
        const char *LinuxStatus::GetFiled(const char *pData,int pos)
        {
            const char *pTemp = pData;
            int len = strlen(pData);
            int count = 0;//空格数－决定块的位置
            if (1 == pos || pos < 1)
            {
                return pTemp;
            }
            int i = 0;
            
            for (i=0; i < len; i++)
            {
                if (' ' == *pTemp)
                {
                    count++;
                    if (count == pos - 1)
                    {
                        pTemp++;
                        break;
                    }
                }
                pTemp++;
            }

            return pTemp;
        }
        uint32_t LinuxStatus::GetProcCPU(const pid_t pid)
        {
            uint32_t used = 0;
            char file[64] = {0};
            ProcessCpuT t;
        
            FILE *fd = NULL;       
            char lineBuff[MAX_FILE_NAME * 4] = {0};   
            sprintf(file,"/proc/%d/stat",pid);
                                                                                                        
            fd = fopen (file, "r"); 
            if (NULL == fd)
            {
                return 0;
            }

            char * end = fgets (lineBuff, sizeof(lineBuff), fd); 
            if (NULL == end)
            {
                return 0;
            }

            sscanf(lineBuff,"%u",&t.Pid);//取得第一项
            const char* data = GetFiled(lineBuff,PROCESS_ITEM);//第14项 起始指针
            sscanf(data,"%u %u %u %u",&t.Utime,&t.Stime,&t.Cutime,&t.Cstime);//格式化14,15,16,17--比整体少了一些项

            fclose(fd);    
            used = t.Utime + t.Stime + t.Cutime + t.Cstime;

            return used;
        }
        uint32_t LinuxStatus::GetTotalCPU()
        {
            uint32_t total = 0;
            FILE * fd = NULL;         
            char buff[1024] = {0};  
            TotalCpuT t;
                                                                                                                    
            fd = fopen ("/proc/stat", "r"); 
            if (NULL == fd)
            {
                return 0;
            }
            char * end = fgets (buff, sizeof(buff), fd); 
            if (NULL == end)
            {
                return 0;
            }
        
            char name[16];
            sscanf (buff, "%s %u %u %u %u", name, &t.User, &t.Nice,&t.System, &t.Idle);

            fclose(fd);     //关闭文件fd
            total = t.User + t.Nice + t.System + t.Idle;

            return total;
        }
        char * LinuxStatus::GetName(char * name, char * p)
        {
            char * t = NULL;

            /*处理小写*/
            while((*p<'a') || (*p>'z'))
            {
                p++;
            }

            if ((t =  strchr(p, ':')))
            {
                memcpy(name, p, t-p);
                return t+1;
            }
            else
            {
                return NULL;
            } 
        }
        int LinuxStatus::GetNetWorkStats(char * ifname,  NetdevStats * pstats)
        {
            FILE * fp = NULL;
            char name[256] = {0};
            char buf[256] = {0};
            char * s = NULL;
            int found = 0;


            if (/*!ifname || */!pstats) 
            {
                return -1;
            }

            fp = fopen(PROC_NET_DEV_FNAME, "r");

            if (!fp) 
            {
                return -1;
            }
            else
            {
                //清除内容的两行头
                char * end = fgets(buf, sizeof(buf), fp );
                if (NULL == end)
                {
                    return -1;
                }
                end = fgets(buf, sizeof(buf), fp );
                if (NULL == end)
                {
                    return -1;
                }

                memset(buf, 0 ,sizeof(buf));

                while (fgets(buf, sizeof(buf), fp ))
                {
                    memset(name, 0, sizeof(name));
                    s = GetName(name, buf);

                    if (s)
                    {
                        sscanf(s, "%llu%llu%lu%lu%lu%lu%lu%lu%llu%llu%lu%lu%lu%lu%lu%lu",
                            &pstats->RxBytesM,  &pstats->RxPacketsM,
                            &pstats->RxErrorsM, &pstats->RxDroppedM,
                            &pstats->RxFifoErrorsM, &pstats->RxFrameErrorsM,
                            &pstats->RxCompressedM,  &pstats->RxMulticastM, 
                            &pstats->TxBytesM, &pstats->TxPacketsM,
                            &pstats->TxErrorsM,&pstats->TxDroppedM,
                            &pstats->TxFifoErrorsM, &pstats->CollisionsM,
                            &pstats->TxCarrierErrorsM,&pstats->TxCompressedM
                            );

                        //判断是否得到相关网卡的信息
                        //if(!strncmp(name, ifname, strlen(ifname))) 
                        if(strncmp(name, "lo", 2))
                        {
                            found = 1;
                            break;              
                        }
                    }
                    else 
                    {
                        continue;
                    }
                }
            
                fclose(fp);
            }

            if (!found)
            {
                return -1;
            }
            else 
            {
                return 0;
            }
        }
        int  LinuxStatus::GetLoadAvg(LoadAvg&lv)
        {
            FILE *fp = NULL;       
            char buf[256] = {0};                                                                                                
            fp = fopen ("/proc/loadavg", "r"); 

            if (NULL == fp)
            {
                return -1;
            }
            else
            {
                char * end = fgets(buf, sizeof(buf), fp );
                if (NULL == end)
                {
                    return -1;
                }
                sscanf(buf, "%lf%lf%lf",&lv.Load1,  &lv.Load5,&lv.Load15);
            }

            fclose(fp);

            return 0;
        }
        uint32_t LinuxStatus::GetCpuNum() 
        {  
            return 32;
            char buf[16] = {0};  
            int num;  
            FILE* fp = popen("cat /proc/cpuinfo |grep processor|wc -l", "r");  
            if(fp) 
            {                                                                                                                                                                               
                int ret = fread(buf, 1, sizeof(buf) - 1, fp);  
                // TODO: check return value
                (void)ret;
                pclose(fp);  
            }
            else
            {
                return 0;
            }     
            sscanf(buf,"%d",&num);  
            if(num <= 0)
            {   
                num = 1;  
            }     
            return (uint32_t)num;  
        } 
        uint32_t LinuxStatus::GetFileCount(pid_t pid)
        {
            DIR *dir = NULL;
            char file[128]={0};
            struct dirent * ptr = NULL;
            uint32_t total = 0;
        
            sprintf(file,"/proc/%d/fd",pid);
            dir = opendir(file); /* 打开目录*/

            if(dir == NULL)
            {
                return 0;
            }

            errno = 0;
            while((ptr = readdir(dir)) != NULL)
            {
                total++;
            }
            if(errno != 0)
            {
                return 0;
            }

            closedir(dir);

            return total;
        }
        bool LinuxStatus::GetPathFromDisk(const char *path,std::string &name)
        {
            bool bRet = true;
            FILE *fp = NULL;       
            char buf[MAX_FILE_NAME * 2] = {0};                                                                                                
            fp = fopen ("/etc/fstab", "r"); 

            if (NULL == fp)
            {
                perror("/etc/fstab:");
                return false;
            }
            else
            {
                char dName[MAX_FILE_NAME] = {0};
                char dPath[MAX_FILE_NAME] = {0};
                while (fgets(buf, sizeof(buf), fp ))
                {
                    sscanf(buf, "%s%s",dName,  dPath);
                    if (!strncmp(path,dPath,strlen(dPath)))
                    {
                        name = std::string(dName);
                    }
                }
            }

            fclose(fp);

            return bRet;
        }
        bool LinuxStatus::GetRWStatus(HardDiskInfo &hdi)
        {
            bool bRet = true;

            DiskRwStatus drs;
            memset(&drs,0,sizeof(drs));

            //读瞬时速度
            drs.Count++;
            drs.TimeStamp = this->GetTimeStamp();

            FILE *fp = NULL;       
            char buf[MAX_FILE_NAME * 4] = {0};                                                                                                
            fp = fopen ("/proc/diskstats", "r"); 

            if (NULL == fp)
            {
                perror("/proc/diskstats:");
                return false;
            }
            else
            {
                char name[MAX_FILE_NAME] = {0};
                uint32_t major = 0;
                uint32_t minor = 0;
                char devname[MAX_FILE_NAME] = {0};
                uint64_t rdios = 0;

                uint64_t rdmerges_or_rdsec = 0;
                uint64_t rdsec_or_wrios = 0;
                uint64_t rdticks_or_wrsec = 0;
                uint64_t wrios = 0;
                uint64_t wrmerges = 0;

                uint64_t wrsec = 0;

                while (fgets(buf, sizeof(buf), fp))
                {
                    sscanf(buf, "%u %u %s %lu %lu %lu %lu %lu %lu %lu",
                    &major, &minor, devname,
                    &rdios, &rdmerges_or_rdsec, &rdsec_or_wrios, &rdticks_or_wrsec,
                    &wrios, &wrmerges, &wrsec);

                    size_t len0 = strlen(name);
                    size_t len1 = strlen(devname);
                    size_t minsize = 0;

                    if (len0 < len1)
                    {
                        minsize = len0;
                    }
                    else
                    {
                        minsize = len1;
                    }

                    if (minsize < 1)
                    {
                        minsize = 1;
                    }

                    //取不同值
                    int result = strncmp(name,devname,minsize);
                    if (result != 0)
                    {
                        drs.CurReadBytesSec += rdsec_or_wrios * 512;
                        drs.CurReadCountSec += rdsec_or_wrios;
                        drs.CurWriteBytesSec += wrsec * 512;
                        drs.CurWriteCountSec += wrsec;
                    }
                    else
                    {
                        continue;
                    }

                    strcpy(name,devname);
                }
                fclose(fp);
            }
            

            //第二次以上开始计算
            if (drs_.Count > 0)
            {
                uint64_t time = (uint64_t) ((drs.TimeStamp - drs_.TimeStamp)*0.001);

                if (time > 0)
                {
                    hdi.ReadCountSec = drs.CurReadCountSec - drs_.CurReadCountSec;
                    hdi.ReadBytesSec = (drs.CurReadBytesSec - drs_.CurReadBytesSec)/time;

                    hdi.WriteCountSec = drs.CurWriteCountSec - drs_.CurWriteCountSec;
                    hdi.WriteBytesSec = (drs.CurWriteBytesSec - drs_.CurWriteBytesSec)/time;
                }
                else
                {
                    return false;
                }
            }

            this->drs_ = drs;

            return bRet;
        }
        uint64_t LinuxStatus::GetTimeStamp()
        {
            struct timeval tv;
            gettimeofday(&tv,NULL);

            uint64_t timestamp = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec *0.001;

            return timestamp;
        }
    }
}
