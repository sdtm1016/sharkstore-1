#include "rangedata.h"
#include <string>
#include <string.h>
#include <sys/types.h>
namespace sharkstore
{
    namespace monitor
    {
        RangeData::RangeData()
        {
            memset(&ds_,0,sizeof(ds_));
        }
        RangeData::~RangeData()
        {

        }

        void RangeData::PutData(RangeTag tag,uint64_t count)
        {
            switch (tag)
            {
                case RangeTag::RangeCount:
                    ds_.RangeCount = count;
                    break;
                case RangeTag::SplitCount:
                    ds_.RangeSplitCount = count;
                    break;
                case RangeTag::SendSnapCount:
                    ds_.SwndingSnapCount = count;
                    break;
                case RangeTag::RecvSnapCount:
                    ds_.ReceivingSnapCount = count;
                    break;
                case RangeTag::ApplySnapCount:
                    ds_.ApplyingSnapCount = count;
                    break;
                case RangeTag::LeaderCount:
                    ds_.RangeLeaderCount = count;
                    break;
                default:
                    return;//error
            }
        }
        void RangeData::GetResult(DsInfo &ds)
        {
            memmove(&ds,&ds_,sizeof(ds));
        }
    }
}