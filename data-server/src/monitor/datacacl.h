#pragma once
#include "syscommon.h"
#include <string>
#include <vector>
#include <sys/types.h>
namespace sharkstore
{
    namespace monitor
    {
        class DataCacl
        {
            public:
                DataCacl();
                ~DataCacl();
            public:
                void PutData(PrintTag tag,uint32_t td);
                void GetResult(CaclResult *pcr);
                void PrintData();
                void GetCurTime(char *buf);
            private:
                void DealPutData(PrintTag tag,uint32_t td);
                void GetPutData(PrintTag tag,uint32_t td);
                void SetPutData(PrintTag tag,uint32_t td);
                void NetPutData(PrintTag tag,uint32_t td);
                void TaskPutData(PrintTag tag,uint32_t td);

                void RaftPutData(PrintTag tag,uint32_t td);
                void StorePutData(PrintTag tag,uint32_t td);
                void QwaitPutData(PrintTag tag,uint32_t td);
                void CaclData(CaclValue *pCacl,CaclResult *pResult);
                void CaclDataDefault(CaclResult *pResult);

                void SavePutData(uint32_t td,CaclValue *pData);
                void ControlData(bool display);
            private:
                void PrintNull(uint32_t index);
                void PrintTagData(PrintTag tag,CaclResult *pData,uint32_t index);
            private:
                PrintTag tag_;
                CaclResult *pResult = nullptr;
        };
    }
}
