#include "datacacl.h"
#include <algorithm>
#include <time.h>

namespace sharkstore
{
    namespace monitor
    {
        //通用数据结构
        CaclValue cv[CACL_ARRAY_LEN];

        DataCacl::DataCacl()
        {
            for (int num = 0;num < CACL_ARRAY_LEN;num++)
            {
                cv[num].PCur = cv[num].FirstBuf;
            }
        }
        DataCacl::~DataCacl()
        {

        }
        void DataCacl::GetResult(CaclResult *pcr)
        {
            if (nullptr != pcr)
            {
                this->pResult = pcr;
                this->ControlData(true);
            }
        }

        void DataCacl::PutData(PrintTag tag,uint32_t timedata)
        {
            switch (tag)
            {
                case PrintTag::Deal:
                    DealPutData(tag,timedata);
                    break;
                case PrintTag::Set:
                    SetPutData(tag,timedata);
                    break;
                case PrintTag::Net:
                    NetPutData(tag,timedata);
                    break;
                case PrintTag::Get:
                    GetPutData(tag,timedata);
                    break;
                case PrintTag::Task:
                    TaskPutData(tag,timedata);
                    break;
                case PrintTag::Raft:
                    RaftPutData(tag,timedata);
                    break;
                case PrintTag::Store:
                    StorePutData(tag,timedata);
		    break;
                case PrintTag::Qwait:
                    QwaitPutData(tag,timedata);
                    break;
                default:
                     return;
            }
        }
        void DataCacl::DealPutData(PrintTag tag,uint32_t timedata)
        {
            //计算TP99等备份
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }
        void DataCacl::GetPutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }
        void DataCacl::SetPutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }
        void DataCacl::NetPutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv + pos);
        }
        void DataCacl::TaskPutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }

        void DataCacl::RaftPutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }
        void DataCacl::StorePutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }
        void DataCacl::QwaitPutData(PrintTag tag,uint32_t timedata)
        {
            int pos = static_cast<int>(tag);
            cv[pos].Tag = tag;
            SavePutData(timedata,cv+pos);
        }
        void DataCacl::SavePutData(uint32_t td,CaclValue *pData)
        {
            pData->Mut.lock();
            uint32_t num = pData->Num++%ARRAY_LEN;
            pData->PCur[num] = td;
            pData->Mut.unlock();
        }

        void DataCacl::PrintData()
        {
            this->ControlData(true);
        }

        void DataCacl::ControlData(bool display)
        {
            for (int num = 0;num < CACL_ARRAY_LEN;num++)
            {
                pResult[num].Tag = cv[num].Tag;
                if (0 == cv[num].Num)
                {
                    if (nullptr != pResult)
                    {
                        CaclDataDefault(pResult+num);
                    }
                    else
                    {
                        printf("store data array is empty!\n");
                    }
                    
                    //控制是否显示
                    if (display)
                    {
                        PrintNull(num);
                    }

                    continue;
                }
                else
                {
                    if (nullptr != pResult)
                    {
                        CaclData(cv+num,pResult+num);

                        if (display)
                        {
                            PrintTagData(pResult[num].Tag,pResult+num,num);
                        }

                    }
                    else
                    {
                        printf("store data array is empty!\n");
                    }
                }
                
            }
        }
        void DataCacl::CaclDataDefault(CaclResult * pResult)
        {
            pResult->Avg = 0.0;
            pResult->Max = 0.0;
            pResult->Min = 0.0;
            pResult->Top50 = 0.0;
            pResult->Top90 = 0.0;
            pResult->Top99 = 0.0;
            pResult->Top999 = 0.0;
        }
        void DataCacl::CaclData(CaclValue *pData,CaclResult *pResult)
        {
            uint64_t tpsCount = 0;
            uint32_t arraylen = 0;

            pData->Mut.lock();
            pData->Swap++;
            uint32_t change = pData->Swap%2;

            if (0 == change)
            {
                pData->PCur = pData->FirstBuf;
                pData->PUseCacl = pData->SecondBuf;
            }
            else
            {
                pData->PCur = pData->SecondBuf;
                pData->PUseCacl = pData->FirstBuf;
            }

            arraylen = pData->Num;

            //判断越界
            if (pData->Num > ARRAY_LEN -1 )
            {
                arraylen = ARRAY_LEN;
            }

            pData->Num = 0;
            pData->Mut.unlock();

            //排序后取出MIN，MAX，AVG
            uint32_t top50 = (uint32_t)(arraylen * 0.5);
            uint32_t top90 = arraylen - (uint32_t)(arraylen * 0.9);
            uint32_t top99 = arraylen - (uint32_t)(arraylen *0.99);
            uint32_t top999 = arraylen - (uint32_t)(arraylen * 0.999);

            //std::greater<uint32_t>()
            std::nth_element(pData->PUseCacl,pData->PUseCacl + top999,pData->PUseCacl + arraylen ,std::greater<uint32_t>());
            pResult->Top999 = pData->PUseCacl[top999]*TIME_UNIT;
            std::nth_element(pData->PUseCacl,pData->PUseCacl + top99,pData->PUseCacl + arraylen,std::greater<uint32_t>() );
            pResult->Top99 = pData->PUseCacl[top99]*TIME_UNIT;
            std::nth_element(pData->PUseCacl,pData->PUseCacl + top90,pData->PUseCacl + arraylen ,std::greater<uint32_t>());
            pResult->Top90 = pData->PUseCacl[top90]*TIME_UNIT;
            std::nth_element(pData->PUseCacl,pData->PUseCacl + top50,pData->PUseCacl + arraylen ,std::greater<uint32_t>());
            pResult->Top50 = pData->PUseCacl[top50]*TIME_UNIT;

            for (uint32_t len = 0;len < arraylen;len++)
            {
                tpsCount += pData->PUseCacl[len];
            }

            auto result = std::minmax_element(pData->PUseCacl,pData->PUseCacl+arraylen);
            pResult->Min = pData->PUseCacl[result.first - pData->PUseCacl] *TIME_UNIT;
            pResult->Max = pData->PUseCacl[result.second - pData->PUseCacl]*TIME_UNIT;
            pResult->Avg = ((1.0*tpsCount)/arraylen)*TIME_UNIT;
            pResult->Len = arraylen;

        }
        void DataCacl::GetCurTime(char *buf)
        {
            time_t puTime;
            time(&puTime);
            tm *gmt = localtime(&puTime);
            strftime(buf, 64, "%Y-%m-%d %H:%M:%S", gmt);
        }
        void DataCacl::PrintNull(uint32_t index)
        {
            printf("***********************************************\n");
            char buf[128] = {0};
            GetCurTime(buf);
	        printf("cur queue is empty!\n");
            printf("cur timestamp values:%s \n",buf);
            printf("%s",TagName[index].c_str());
            printf("\n");

            printf("top999 values:%f ms\n",0.0);
            printf("top99 values:%f ms\n",0.0);
            printf("top90 values:%f ms\n",0.0);
            printf("top50 values:%f ms\n",0.0);

            printf("min values:%f ms\n",0.0);
            printf("max values:%f ms\n",0.0);
            printf( "avg values:%f ms\n",0.0);
            printf("***********************************************\n");
        }
        void DataCacl::PrintTagData(PrintTag tag,CaclResult *pData,uint32_t index)
        {
            printf("***********************************************\n");
            char buf[128] = {0};
            GetCurTime(buf);
            printf("cur timestamp values:%s\n",buf);
            printf("cur queue length is:%u \n",pData->Len);
            printf("%s",TagName[index].c_str());
            printf("\n");
            printf( "top999 values:%0.3f ms\n",pData->Top999 * DISP_UNIT);
            printf( "top99 values:%0.3f ms\n",pData->Top99 * DISP_UNIT);
            printf( "top90 values:%0.3f ms\n",pData->Top90* DISP_UNIT);
            printf( "top50 values:%0.3f ms\n",pData->Top50* DISP_UNIT);

            printf("min values:%0.3f ms\n",pData->Min*DISP_UNIT);
            printf("max values:%0.3f ms\n",pData->Max*DISP_UNIT);
            printf( "avg values:%0.3f ms\n",pData->Avg*DISP_UNIT);
            printf("***********************************************\n");
        }
    }
}
