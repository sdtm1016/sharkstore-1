_Pragma("once");

#include <atomic>
#include "rocksdb/db.h"
#include "base/status.h"
#include "base/util.h"
#include "storage/meta_store.h"
//#include "storage/store.h"
#include <iostream>
#include <mutex>

#define DEFAULT_CACHE_SIZE 1

namespace sharkstore {

class IdGenerater {
public:
    IdGenerater() {
        ;
    }
    IdGenerater(const uint64_t &range_id, const uint32_t &cache, sharkstore::dataserver::storage::MetaStore *meta):
                cache_(cache),range_id_(range_id),store_(meta),saveCnt_(0) {
        init();
    }

    ~IdGenerater(){
        //to do save number to rocksdb
        store_ = nullptr;
    }

    void init() {
        //to do get from rocksdb and save newId to db
        initFlag = false;

        Status ret;
        int64_t tmpSeqId(0);

        ret = store_->GetVersionID(range_id_, &tmpSeqId);
        if(ret.ok()) {
            seq_.fetch_add(tmpSeqId);
            seq_end_ += seq_ + cache_;
            //std::cout << ">>>>>>seq_:" << seq_ << "end:" << seq_end_ << "tmpseq:" << tmpSeqId << std::endl;
            uint64_t tmpId = seq_;
            //FLOG_DEBUG("seq_[%" PRIu64 "] seq_end_[%" PRIu64 "]", tmpId, seq_end_);

            if( 0 == saveToDb(seq_end_) ) initFlag = true;
        } else {
            initFlag = false;
            setErrMsg(ret.ToString());
            int64_t tmpId = seq_;
            //FLOG_DEBUG("init fail and default value of seq_[%" PRIu64 "] seq_end_[%" PRIu64 "]", tmpId, seq_end_);
        }

        return;
    }

    void setRangeId(const uint64_t &rangeId) {
        range_id_ = rangeId;
    }
    uint64_t &getRangeId() {
        return range_id_;
    }
    void setCache() {
        setCache(DEFAULT_CACHE_SIZE);
    }
    void setCache(const uint32_t &cacheNum) {
        cache_ = cacheNum;
    }

    int16_t currentId(int64_t *id ) {
        int16_t ret(0);

        if (initFlag) {
            if (id == nullptr) {
                id = new int64_t(0L);
            }

            auto retSts = store_->GetVersionID(range_id_, id);
            if(!retSts.ok()) {
                *id = 0;
                ret = -1;
                setErrMsg( "Generator getVersionID failure." );
            }

        } else {
            *id = 0;
            ret = -1;
            setErrMsg( "Generator is not init yet or encournter init failure." );
        }

        return ret;
    }

    int16_t nextId(int64_t *id ) {
        int16_t ret(0);
        
        if (initFlag) {
            if (id == nullptr) {
                id = new int64_t(0L);
            }

            seq_.fetch_add(1);
            *id = seq_;

            if (static_cast<uint64_t>(*id) >= seq_end_) {
                seq_end_ = static_cast<uint64_t>(*id) + cache_;
                ret = saveToDb(seq_end_);

                if (0 == ret) {
                    saveCnt_++;
                }
            }

        } else {
            setErrMsg( "Generator is not init yet or encournter init failure." );
            ret = -1;
        }

        return ret;
    }

    std::string &getErrMsg() {
        return errMsg;
    }
    void clearErrMsg() noexcept {
        errMsg.assign("");
    }
    
    bool isInit() {
        return initFlag;
    }
private:
    int16_t saveToDb(const uint64_t &futureId) {
        //std::unique_lock<std::mutex> lock(mtx_);
        int16_t retCode(0);

        {
            //to do save to rocksdb
            Status ret = store_->SaveVersionID(range_id_, futureId);
            if (!ret.ok()) {
                setErrMsg( ret.ToString() );
            }
            if (ret.ok()) {
                retCode = 0;
            } else {
                retCode = -1;
            }
        }

        return retCode;
    }

    void setErrMsg(const std::string &msg) {
        errMsg.assign(msg);
    }

private:
    std::atomic<int64_t> seq_{0};
    uint64_t seq_end_ = 0;
    uint32_t cache_ = 0;

    uint64_t range_id_ = 0;
    std::string range_key_{""};

    sharkstore::dataserver::storage::MetaStore *store_ = nullptr;
    
    bool initFlag = false;
    std::string errMsg;
    uint32_t saveCnt_ = 0;

    std::mutex mtx_;
};

} //end namespace
