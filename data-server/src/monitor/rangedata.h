#pragma once
#include "syscommon.h"
#include <string>
#include <vector>
#include <sys/types.h>
namespace sharkstore
{
    namespace monitor
    {
        class RangeData
        {
            public:
                RangeData();
                ~RangeData();
            public:
                void PutData(RangeTag tag, uint64_t count);
                void GetResult(DsInfo &ds);
            private:
                DsInfo ds_;
        };
    }
}