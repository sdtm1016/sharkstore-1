#include "encodedata.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

namespace sharkstore
{
    namespace monitor
    {
        EncodeData::EncodeData()
        {
            
        }
        EncodeData::~EncodeData()
        {

        }
        void EncodeData::EncodeToJson(ProcessStats & ps)
        {

        }

        void EncodeData::Output(rapidjson::Document& d,std::string &str)
        {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);
            str = buffer.GetString();
        }
        void EncodeData::GetDiskMem(ProcessStats &pStats,rapidjson::Document& doc)
        {
            rapidjson::StringBuffer sDisk;
            rapidjson::Writer<rapidjson::StringBuffer> writerDisk(sDisk);
            writerDisk.StartObject();

            writerDisk.Key("disk_path");
            writerDisk.String(pStats.HardDisk.Name);
            writerDisk.Key("disk_free");
            writerDisk.Uint64(pStats.HardDisk.Free);
            writerDisk.Key("disk_total");
            writerDisk.Uint64(pStats.HardDisk.Total);
            writerDisk.Key("disk_proc_rate");
            writerDisk.Double(pStats.HardDisk.Rate);
            writerDisk.Key("disk_read_byte_per_sec");
            writerDisk.Uint64(pStats.HardDisk.ReadBytesSec);
            writerDisk.Key("disk_write_byte_per_sec");
            writerDisk.Uint64(pStats.HardDisk.WriteBytesSec);
            writerDisk.Key("disk_read_count_per_sec");
            writerDisk.Uint64(pStats.HardDisk.ReadCountSec);
            writerDisk.Key("disk_write_count_per_sec");
            writerDisk.Uint64(pStats.HardDisk.WriteCountSec);
            writerDisk.EndObject();

            //disk info
            doc.Parse(sDisk.GetString());
        }
        void EncodeData::GetTpMem(ProcessStats &pStats,rapidjson::Document& doc)
        {
            rapidjson::StringBuffer sTp;
            rapidjson::Writer<rapidjson::StringBuffer> writerTp(sTp);
            writerTp.StartObject();

            writerTp.Key("min");
            writerTp.Double(pStats.Tp.Min);
            writerTp.Key("max");
            writerTp.Double(pStats.Tp.Max);

            writerTp.Key("avg");
            writerTp.Double(pStats.Tp.Avg);
            writerTp.Key("tp_50");
            writerTp.Double(pStats.Tp.Tp50);

            writerTp.Key("tp_90");
            writerTp.Double(pStats.Tp.Tp90);
            writerTp.Key("tp_99");
            writerTp.Double(pStats.Tp.Tp99);

            writerTp.Key("tp_999");
            writerTp.Double(pStats.Tp.Tp999);
    
            writerTp.EndObject();

            //tpstats info
            doc.Parse(sTp.GetString());
        }
        void EncodeData::GetDsMem(ProcessStats &pStats,rapidjson::Document& doc)
        {
            rapidjson::StringBuffer sDs;
            rapidjson::Writer<rapidjson::StringBuffer> writerDs(sDs);
            writerDs.StartObject();

            writerDs.Key("range_count");
            writerDs.Uint64(pStats.Ds.RangeCount);
            writerDs.Key("range_split_count");
            writerDs.Uint64(pStats.Ds.RangeSplitCount);
            writerDs.Key("sending_snap_count");
            writerDs.Uint64(pStats.Ds.SwndingSnapCount);
            writerDs.Key("receiving_snap_count");
            writerDs.Uint64(pStats.Ds.ReceivingSnapCount);
            writerDs.Key("applying_snap_count");
            writerDs.Uint64(pStats.Ds.ApplyingSnapCount);
            writerDs.Key("range_leader_count");
            writerDs.Uint64(pStats.Ds.RangeLeaderCount);
            writerDs.Key("version");
            writerDs.String(pStats.Ds.Ver);

            writerDs.EndObject();

            //dsinfo
            doc.Parse(sDs.GetString());
        }
        void EncodeData::SysInfoToJson(ProcessStats &pStats,std::string &info)
        {
            //root-ProcessStats
            rapidjson::StringBuffer sPstats;
            rapidjson::Writer<rapidjson::StringBuffer> writer(sPstats);
            writer.StartObject();

            writer.Key("cpu_proc_rate");
            writer.Double(pStats.CpuRate);
            writer.Key("memory_total");
            writer.Uint64(pStats.Mem.Total);
            writer.Key("memory_used");
            writer.Uint64(pStats.Mem.UsedRss);

            writer.Key("connect_count");
            writer.Uint64(0);  //目前没有
            writer.Key("thread_num");
            writer.Uint(pStats.ThreadNum);
            writer.Key("handle_num");
            writer.Uint(pStats.HandleNum);
            writer.Key("start_time");
            writer.Uint64(pStats.StartTime);

            //second-1:HardDisk
            rapidjson::Document dDisk;
            this->GetDiskMem(pStats,dDisk);

            //second-2:TpStats
            rapidjson::Document dTp;
            this->GetTpMem(pStats,dTp);

            //second-3 dsinfo
            rapidjson::Document dDs;
            this->GetDsMem(pStats,dDs);
                
            //all end
            writer.EndObject(); 
           
            //root
            const char *root = sPstats.GetString();
            rapidjson::Document doc; 
            doc.Parse(root);
            rapidjson::Document::AllocatorType& alloc = doc.GetAllocator();

            //insert data
            doc.AddMember("disk_stats",dDisk,alloc);
            doc.AddMember("tp_stats",dTp,alloc);
            doc.AddMember("ds_info",dDs,alloc);
            Output(doc,info);
        }
    }
}