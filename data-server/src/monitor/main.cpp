
#include "isystemstatus.h"
#include <iostream>
#include <unistd.h>
#include <thread>
#include <unistd.h>
#include <time.h>
using namespace sharkstore::monitor;

int main(int argc,char **argv)
{
    MemInfo meminfo;
    ISystemStatus is;
    is.GetMemInfo(meminfo);
    pid_t pid = getpid();


    srand((int)time(0));
    std::thread handle1 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*1000;num++)
        {
            usleep(1000);
            //std::cout << "Hello from lamda thread " << std::this_thread::get_id() << std::endl;
            uint32_t td = (int)(50.0*rand()/(RAND_MAX+1.0)) + num;
            is.PutTopData(PrintTag::Qwait,td);
        }
        
    });

    std::thread handle11 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*10;num++)
        {
            usleep(1000);
            //std::cout << "Hello from lamda thread " << std::this_thread::get_id() << std::endl;
            uint32_t td = (int)(50.0*rand()/(RAND_MAX+1.0)) + num;
            is.PutTopData(PrintTag::Qwait,td);
        }
        
    });

    std::thread handle111 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*10;num++)
        {
            usleep(1000);
            //std::cout << "Hello from lamda thread " << std::this_thread::get_id() << std::endl;
            uint32_t td = (int)(50.0*rand()/(RAND_MAX+1.0)) + num;
            is.PutTopData(PrintTag::Qwait,td);
        }
        
    });


    std::thread handle2 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*1000*1000;num++)
        {
            usleep(1000);
            uint32_t td = num - (int)(50.0*rand()/(RAND_MAX+1.0)) + 500;
            is.PutTopData(PrintTag::Task,td);
        }
        
    });

    std::thread handle3 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*1000*1000;num++)
        {
            usleep(1000);
            uint32_t td = num + 50 -  (int)(50.0*rand()/(RAND_MAX+1.0)) ;
            is.PutTopData(PrintTag::Set,td);
        }
        
    });

    std::thread handle4 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*1000*1000;num++)
        {
            usleep(1000);
            uint32_t td = num + 150 -  (int)(50.0*rand()/(RAND_MAX+1.0)) ;
            is.PutTopData(PrintTag::Deal,td);
        }
        
    });

    std::thread handle5 = std::thread([&]{
        for (uint32_t num = 0;num < 1000*1000*1000;num++)
        {
            usleep(1000);
            uint32_t td = num + 850 -  (int)(50.0*rand()/(RAND_MAX+1.0)) ;
            is.PutTopData(PrintTag::Qwait,td);
        }
        
    });

    HardDiskInfo hdi;
    is.GetHardDiskInfo(hdi);
    std::cout<<"hard info free value:"<<hdi.Free<<std::endl;
    std::cout<<"hard info total value:"<<hdi.Total<<std::endl;
    std::cout<<"hard info used value:"<<hdi.Used<<std::endl;
    std::cout<<"hard info rate value:"<<hdi.Rate<<std::endl;
    std::string str;
    is.GetProcessStats(str);

    std::cout<<str<<std::endl;
    std::cout<<"mem threadcoutn vlaue :"<<meminfo.ThreadCount<<std::endl;
    std::cout<<"mem total value:"<<meminfo.Total<<std::endl;
    std::cout<<"mem free value:"<<meminfo.Free << std::endl;
    std::cout<<"mem use value:"<<meminfo.Used << std::endl;
    std::cout<<"mem rate value:"<<meminfo.Rate<<std::endl;

    int size = sizeof(long long);
    size = sizeof(long);
    size = sizeof(int);
    size = sizeof(int32_t);

    
    std::thread hGet = std::thread([&]{
        for (uint32_t num = 0;num < 1000*1000*1000;num++)
        {
            sleep(10);
            std::string str;
            is.GetProcessStats(str);
        }
        
    });


    //system("pause");
    while(true){sleep(1);}

    return 0;
}