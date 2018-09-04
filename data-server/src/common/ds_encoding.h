#ifndef FBASE_DATASERVER_ENCODING_H_
#define FBASE_DATASERVER_ENCODING_H_

#include <stdint.h>
#include <string>
#include <vector>

namespace sharkstore {
namespace dataserver {

static const uint32_t kNoColumnID = 0;

enum class EncodeType : char {
    Unknown = 0,
    Null,
    NotNull,
    Int,
    Float,
    Decimal,
    Bytes,
    BytesDesc,  // Bytes encoded descendingly
    Time,
    Duration,
    True,
    False,

    SentinelType = 15  // Used in the Value encoding.
};

void EncodeNonSortingUvarint(std::string* buf, uint64_t value);
void EncodeNonSortingVarint(std::string* buf, int64_t value);
bool DecodeNonSortingUvarint(const std::string& data, size_t& offset, uint64_t* value);
bool DecodeNonSortingVarint(const std::string& data, size_t& offset, int64_t* value);

void EncodeUint64Ascending(std::string* buf, uint64_t value);
bool DecodeUint64Ascending(const std::string& data, size_t& offset, uint64_t* value);

void EncodeIntValue(std::string* buf, uint32_t col_id, int64_t value);
void EncodeFloatValue(std::string* buf, uint32_t col_id, double value);
void EncodeBytesValue(std::string* buf, uint32_t col_id, const char* value, size_t value_size);
void EncodeNullValue(std::string* buf, uint32_t col_id);

// offset: 输入&输出参数 从data的offset处开始解码，并返回剩余数据的offset
bool DecodeValueTag(const std::string& data, size_t& offset, uint32_t* col_id, EncodeType* type);
bool DecodeIntValue(const std::string& data, size_t& offset, int64_t* value);
bool DecodeFloatValue(const std::string& data, size_t& offset, double* value);
bool DecodeBytesValue(const std::string& data, size_t& offset, std::string* value);
bool SkipValue(const std::string& data, size_t& offset);

void EncodeUvarintAscending(std::string* buf, uint64_t value);
void EncodeVarintAscending(std::string* buf, int64_t value);
void EncodeFloatAscending(std::string* buf, double value);
void EncodeBytesAscending(std::string* buf, const char* value, size_t value_size);

bool DecodeUvarintAscending(const std::string& buf, size_t& pos, uint64_t* out);
bool DecodeVarintAscending(const std::string& buf, size_t& pos, int64_t* out);
bool DecodeFloatAscending(const std::string& buf, size_t& pos, double* out);
bool DecodeBytesAscending(const std::string& buf, size_t& pos, std::string* out);

// for tests or debug
std::string EncodeToHexString(const std::string& str);

} /* namespace dataserver */
} /* namespace sharkstore */

#endif /* end of include guard: FBASE_DATASERVER_ENCODING_H_ */
