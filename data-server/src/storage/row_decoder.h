_Pragma("once");

#include <map>
#include <string>

#include "base/status.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

struct FieldValue;

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult&) = delete;
    RowResult& operator=(const RowResult&) = delete;

    bool AddField(uint64_t col, FieldValue* fval);
    FieldValue* GetField(uint64_t col) const;

    void SetKey(const std::string& key) { key_ = key; }
    const std::string& Key() const { return key_; }

    // 清空，方便迭代时重用
    void Reset();

private:
    std::string key_;
    std::map<uint64_t, FieldValue*> fields_;
};

class RowDecoder {
public:
    RowDecoder(
        const std::vector<metapb::Column>& primary_keys,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches);

    RowDecoder(
        const std::vector<metapb::Column>& primary_keys,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::SelectField>&
            field_list,
        const ::google::protobuf::RepeatedPtrField< ::kvrpcpb::Match>& matches);

    ~RowDecoder();

    RowDecoder(const RowDecoder&) = delete;
    RowDecoder& operator=(const RowDecoder&) = delete;

    Status Decode(const std::string& key, const std::string& buf,
                  RowResult* result);

    Status DecodeAndFilter(const std::string& key, const std::string& buf,
                           RowResult* result, bool* matched);

    std::string DebugString() const;

private:
    Status decodePrimaryKeys(const std::string& key, RowResult* result);

private:
    const std::vector<metapb::Column>& primary_keys_;
    std::map<uint64_t, metapb::Column> cols_;
    std::vector<kvrpcpb::Match> filters_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
