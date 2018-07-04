#pragma once
#include "syscommon.h"
#include <string>
#include <sys/types.h>
#include "rapidjson/document.h"

namespace sharkstore
{
    namespace monitor
    {
        class EncodeData
        {
            public:
                EncodeData();
                ~EncodeData();
            public:
                void EncodeToJson(ProcessStats & ps);
                void Output(rapidjson::Document& d,std::string &str);
                void SysInfoToJson(ProcessStats &pStats,std::string &info);
            private:
                void GetDiskMem(ProcessStats &pStats,rapidjson::Document& doc);
                void GetTpMem(ProcessStats &pStats,rapidjson::Document& doc);
                void GetDsMem(ProcessStats &pStats,rapidjson::Document& doc);
        };
    }
}