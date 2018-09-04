#include "ds_encoding.h"

#include <assert.h>
#include <string.h>
#include <cmath>
#include <iostream>

namespace sharkstore {
namespace dataserver {

void EncodeNonSortingUvarint(std::string *buf, uint64_t x) {
    static const uint64_t b1 = static_cast<uint64_t>(1) << 7;
    static const uint64_t b2 = static_cast<uint64_t>(1) << 14;
    static const uint64_t b3 = static_cast<uint64_t>(1) << 21;
    static const uint64_t b4 = static_cast<uint64_t>(1) << 28;
    static const uint64_t b5 = static_cast<uint64_t>(1) << 35;
    static const uint64_t b6 = static_cast<uint64_t>(1) << 42;
    static const uint64_t b7 = static_cast<uint64_t>(1) << 49;
    static const uint64_t b8 = static_cast<uint64_t>(1) << 56;
    static const uint64_t b9 = static_cast<uint64_t>(1) << 63;

    if (x < b1) {
        buf->push_back(static_cast<char>(x));
    } else if (x < b2) {
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b3) {
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b4) {
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b5) {
        buf->push_back(static_cast<char>(x >> 28 | 0x80));
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b6) {
        buf->push_back(static_cast<char>(x >> 35 | 0x80));
        buf->push_back(static_cast<char>(x >> 28 | 0x80));
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b7) {
        buf->push_back(static_cast<char>(x >> 42 | 0x80));
        buf->push_back(static_cast<char>(x >> 35 | 0x80));
        buf->push_back(static_cast<char>(x >> 28 | 0x80));
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b8) {
        buf->push_back(static_cast<char>(x >> 49 | 0x80));
        buf->push_back(static_cast<char>(x >> 42 | 0x80));
        buf->push_back(static_cast<char>(x >> 35 | 0x80));
        buf->push_back(static_cast<char>(x >> 28 | 0x80));
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else if (x < b9) {
        buf->push_back(static_cast<char>(x >> 56 | 0x80));
        buf->push_back(static_cast<char>(x >> 49 | 0x80));
        buf->push_back(static_cast<char>(x >> 42 | 0x80));
        buf->push_back(static_cast<char>(x >> 35 | 0x80));
        buf->push_back(static_cast<char>(x >> 28 | 0x80));
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    } else {
        buf->push_back(static_cast<char>(x >> 63 | 0x80));
        buf->push_back(static_cast<char>(x >> 56 | 0x80));
        buf->push_back(static_cast<char>(x >> 42 | 0x80));
        buf->push_back(static_cast<char>(x >> 49 | 0x80));
        buf->push_back(static_cast<char>(x >> 35 | 0x80));
        buf->push_back(static_cast<char>(x >> 28 | 0x80));
        buf->push_back(static_cast<char>(x >> 21 | 0x80));
        buf->push_back(static_cast<char>(x >> 14 | 0x80));
        buf->push_back(static_cast<char>(x >> 7 | 0x80));
        buf->push_back(static_cast<char>(x & 0x7f));
    }
}

bool DecodeNonSortingUvarint(const std::string &data, size_t &offset, uint64_t *value) {
    uint8_t b = 0;
    *value = 0;
    for (; offset < data.size(); ++offset) {
        b = static_cast<uint8_t>(data[offset]);
        *value <<= 7;
        *value += static_cast<uint64_t>(b & 0x7f);
        if (b < 0x80) {
            ++offset;
            return true;
        }
    }
    return false;
}

void EncodeNonSortingVarint(std::string *buf, int64_t value) {
    uint64_t uv = static_cast<uint64_t>(value) << 1;
    if (value < 0) {
        uv = ~uv;
    }

    while (uv >= 0x80) {
        buf->push_back(static_cast<char>(uv | 0x80));
        uv >>= 7;
    }
    buf->push_back(static_cast<char>(uv));
}

bool DecodeNonSortingVarint(const std::string &data, size_t &offset, int64_t *value) {
    uint64_t ux = 0;
    uint8_t b = 0, s = 0;
    bool flag = false;
    for (; offset < data.size(); ++offset) {
        b = static_cast<uint8_t>(data[offset]);
        if (b < 0x80) {
            ux |= (static_cast<uint64_t>(b) << s);
            ++offset;
            flag = true;
            break;
        }
        ux |= (static_cast<uint64_t>(b & 0x7f) << s);
        s += 7;
    }
    if (!flag) {
        return false;
    }

    *value = static_cast<int64_t>(ux >> 1);
    if ((ux & 1) != 0) {
        *value = ~(*value);
    }
    return true;
}

void encodeValueTag(std::string *buf, uint32_t col_id, EncodeType type) {
    if (type >= EncodeType::SentinelType) {
        EncodeNonSortingUvarint(
            buf,
            (static_cast<uint64_t>(col_id) << 4) | static_cast<uint64_t>(EncodeType::SentinelType));
        EncodeNonSortingUvarint(buf, static_cast<uint64_t>(type));
        return;
    }
    if (kNoColumnID == col_id) {
        buf->push_back(static_cast<char>(type));
        return;
    }
    EncodeNonSortingUvarint(buf,
                            (static_cast<uint64_t>(col_id) << 4) | static_cast<uint64_t>(type));
}

void EncodeUint64Ascending(std::string *buf, uint64_t value) {
    buf->push_back(static_cast<char>(value >> 56));
    buf->push_back(static_cast<char>(value >> 48));
    buf->push_back(static_cast<char>(value >> 40));
    buf->push_back(static_cast<char>(value >> 32));
    buf->push_back(static_cast<char>(value >> 24));
    buf->push_back(static_cast<char>(value >> 16));
    buf->push_back(static_cast<char>(value >> 8));
    buf->push_back(static_cast<char>(value));
}

bool DecodeUint64Ascending(const std::string &data, size_t &offset, uint64_t *value) {
    if (offset + 8 > data.size()) {
        return false;
    }
    *value = 0;
    for (int i = 0; i < 8; i++) {
        *value <<= 8;
        *value += static_cast<uint8_t>(data[offset + i]);
    }
    offset += 8;
    return true;
}

void EncodeIntValue(std::string *buf, uint32_t col_id, int64_t value) {
    assert(buf != nullptr);
    encodeValueTag(buf, col_id, EncodeType::Int);
    EncodeNonSortingVarint(buf, value);
}

void EncodeFloatValue(std::string *buf, uint32_t col_id, double value) {
    assert(buf != nullptr);

    encodeValueTag(buf, col_id, EncodeType::Float);

    uint64_t x = 0;
    assert(sizeof(double) == sizeof(uint64_t));
    memcpy(&x, &value, sizeof(double));
    EncodeUint64Ascending(buf, x);
}

void EncodeBytesValue(std::string *buf, uint32_t col_id, const char *value, size_t value_size) {
    assert(buf != nullptr);
    encodeValueTag(buf, col_id, EncodeType::Bytes);
    EncodeNonSortingUvarint(buf, static_cast<uint64_t>(value_size));
    buf->append(value, value_size);
}

void EncodeNullValue(std::string *buf, uint32_t col_id) {
    encodeValueTag(buf, col_id, EncodeType::Null);
}

bool DecodeValueTag(const std::string &data, size_t &offset, uint32_t *col_id, EncodeType *type) {
    if (offset >= data.size()) {
        return false;
    }

    uint64_t tag = 0;
    if (!DecodeNonSortingUvarint(data, offset, &tag)) {
        return false;
    }

    *col_id = static_cast<uint32_t>(tag >> 4);
    *type = static_cast<EncodeType>(tag & 0x0f);
    if (EncodeType::SentinelType == *type) {
        if (!DecodeNonSortingUvarint(data, offset, &tag)) {
            return false;
        }
        *type = static_cast<EncodeType>(tag);
    }
    return true;
}

bool decodeValueTypeAssert(const std::string &data, size_t &offset, EncodeType expect_type) {
    uint32_t col_id = 0;
    EncodeType type = EncodeType::Unknown;
    if (!DecodeValueTag(data, offset, &col_id, &type)) {
        return false;
    }
    return type == expect_type;
}

bool DecodeIntValue(const std::string &data, size_t &offset, int64_t *value) {
    assert(value != nullptr);
    if (!decodeValueTypeAssert(data, offset, EncodeType::Int)) {
        return false;
    }
    return DecodeNonSortingVarint(data, offset, value);
}

bool DecodeFloatValue(const std::string &data, size_t &offset, double *value) {
    assert(value != nullptr);
    if (!decodeValueTypeAssert(data, offset, EncodeType::Float)) {
        return false;
    }
    uint64_t x = 0;
    assert(sizeof(uint64_t) == sizeof(double));
    if (!DecodeUint64Ascending(data, offset, &x)) {
        return false;
    }
    memcpy(value, &x, sizeof(x));
    return true;
}

bool DecodeBytesValue(const std::string &data, size_t &offset, std::string *value) {
    assert(value != nullptr);
    if (!decodeValueTypeAssert(data, offset, EncodeType::Bytes)) {
        return false;
    }
    uint64_t len = 0;
    if (!DecodeNonSortingUvarint(data, offset, &len)) {
        return false;
    }
    if (offset + len > data.size()) {
        return false;
    }
    *value = data.substr(offset, len);
    offset += len;
    return true;
}

bool SkipValue(const std::string &data, size_t &offset) {
    uint32_t col_id = 0;
    EncodeType type;
    if (!DecodeValueTag(data, offset, &col_id, &type)) {
        return false;
    }

    switch (type) {
        case EncodeType::Int: {
            int64_t value;
            return DecodeNonSortingVarint(data, offset, &value);
        }
        case EncodeType::Float:
            offset += sizeof(uint64_t);
            return offset <= data.size();
        case EncodeType::Bytes: {
            uint64_t len = 0;
            if (!DecodeNonSortingUvarint(data, offset, &len)) {
                return false;
            }
            offset += len;
            return offset <= data.size();
        }
        default:
            return false;
    }
}

static const uint8_t kIntMin = 0x80;
static const uint8_t kIntMax = 0xfd;
static const uint8_t kIntMaxWidth = 8;
static const uint8_t kIntZero = kIntMin + kIntMaxWidth;
static const uint8_t kIntSmall = kIntMax - kIntZero - kIntMaxWidth;  // 109

void EncodeUvarintAscending(std::string *buf, uint64_t value) {
    if (value <= kIntSmall) {
        buf->push_back(static_cast<char>(kIntZero + value));
    } else if (value <= 0xff) {
        buf->push_back(static_cast<char>(kIntMax - 7));
        buf->push_back(static_cast<char>(value));
    } else if (value <= 0xffff) {  // 2
        buf->push_back(static_cast<char>(kIntMax - 6));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value <= 0xffffff) {  // 3
        buf->push_back(static_cast<char>(kIntMax - 5));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value <= 0xffffffff) {  // 4
        buf->push_back(static_cast<char>(kIntMax - 4));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value <= 0xffffffffff) {  // 5
        buf->push_back(static_cast<char>(kIntMax - 3));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value <= 0xffffffffffff) {  // 6
        buf->push_back(static_cast<char>(kIntMax - 2));
        buf->push_back(static_cast<char>(value >> 40));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value <= 0xffffffffffffff) {  // 7
        buf->push_back(static_cast<char>(kIntMax - 1));
        buf->push_back(static_cast<char>(value >> 48));
        buf->push_back(static_cast<char>(value >> 40));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else {
        buf->push_back(static_cast<char>(kIntMax));
        buf->push_back(static_cast<char>(value >> 56));
        buf->push_back(static_cast<char>(value >> 48));
        buf->push_back(static_cast<char>(value >> 40));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    }
}

void EncodeVarintAscending(std::string *buf, int64_t value) {
    if (value >= 0) {
        return EncodeUvarintAscending(buf, static_cast<uint64_t>(value));
    }

    if (value >= -0xff) {
        buf->push_back(static_cast<char>(kIntMin + 7));
        buf->push_back(static_cast<char>(value));
    } else if (value >= -0xffff) {
        buf->push_back(static_cast<char>(kIntMin + 6));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value >= -0xffffff) {
        buf->push_back(static_cast<char>(kIntMin + 5));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value >= -0xffffffff) {
        buf->push_back(static_cast<char>(kIntMin + 4));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value >= -0xffffffffff) {
        buf->push_back(static_cast<char>(kIntMin + 3));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value >= -0xffffffffffff) {
        buf->push_back(static_cast<char>(kIntMin + 2));
        buf->push_back(static_cast<char>(value >> 40));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else if (value >= -0xffffffffffffff) {
        buf->push_back(static_cast<char>(kIntMin + 1));
        buf->push_back(static_cast<char>(value >> 48));
        buf->push_back(static_cast<char>(value >> 40));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    } else {
        buf->push_back(static_cast<char>(kIntMin));
        buf->push_back(static_cast<char>(value >> 56));
        buf->push_back(static_cast<char>(value >> 48));
        buf->push_back(static_cast<char>(value >> 40));
        buf->push_back(static_cast<char>(value >> 32));
        buf->push_back(static_cast<char>(value >> 24));
        buf->push_back(static_cast<char>(value >> 16));
        buf->push_back(static_cast<char>(value >> 8));
        buf->push_back(static_cast<char>(value));
    }
}

static const uint8_t kFloatNaN = 0x00 + 1;
static const uint8_t kFloatNeg = kFloatNaN + 1;
static const uint8_t kFloatZero = kFloatNeg + 1;
static const uint8_t kFloatPos = kFloatZero + 1;

void EncodeFloatAscending(std::string *buf, double value) {
    uint64_t u = 0;
    assert(sizeof(u) == sizeof(double));
    memcpy(&u, &value, sizeof(double));
    if (std::isnan(u)) {
        buf->push_back(static_cast<char>(kFloatNaN));
        return;
    } else if (u == 0) {
        buf->push_back(static_cast<char>(kFloatZero));
        return;
    }
    if ((u & (static_cast<uint64_t>(1) << 63)) != 0) {
        u = ~u;
        buf->push_back(static_cast<char>(kFloatNeg));
    } else {
        buf->push_back(static_cast<char>(kFloatPos));
    }
    EncodeUint64Ascending(buf, u);
}

static const uint8_t kBytesMarker = 0x12;
static const uint8_t kEscape = 0x00;
static const uint8_t kEscapedTerm = 0x01;
static const uint8_t kEscaped00 = 0xff;
static const uint8_t kEscapedFF = 0x00;

void EncodeBytesAscending(std::string *buf, const char *value, size_t value_size) {
    buf->push_back(static_cast<char>(kBytesMarker));
    for (size_t i = 0; i < value_size; i++) {
        buf->push_back(value[i]);
        if (static_cast<uint8_t>(value[i]) == kEscape) {
            buf->push_back(kEscaped00);
        }
    }
    buf->push_back(kEscape);
    buf->push_back(kEscapedTerm);
}

bool DecodeUvarintAscending(const std::string& buf, size_t& pos, uint64_t* out) {
    if (pos >= buf.size()) return false;
    int len = (int)(unsigned char)buf[pos] - kIntZero;
    if (len <= kIntSmall) {
        ++pos;
        if (out) *out = len;
    } else {
        len -= kIntSmall;
        if (len < 0 || len > 8 || pos + len >= buf.size()) return false;
        uint64_t value = 0;
        for (++pos; len > 0; ++pos, --len) value = value << 8 | (unsigned char)buf[pos];
        if (out) *out = value;
    }
    return true;
}

bool DecodeVarintAscending(const std::string& buf, size_t& pos, int64_t* out) {
    if (pos >= buf.size()) return false;
    int len = (int)(unsigned char)buf[pos] - kIntZero;
    if (len > 0) {
        return DecodeUvarintAscending(buf, pos, reinterpret_cast<uint64_t *>(out));
    }  else if (len == 0) {
        ++pos;
        if (out) *out = 0;
        return true;
    } else {
        len = -len;
        if (pos + len >= buf.size()) return false;
        int64_t value = -1;
        for (++pos; len > 0; ++pos, --len) value = value << 8 | (unsigned char)buf[pos];
        if (out) *out = value;
        return true;
    }
}

bool DecodeFloatAscending(const std::string& buf, size_t& pos, double* out) {
    return false;
}

bool DecodeBytesAscending(const std::string& buf, size_t& pos, std::string* out) {
    if (pos >= buf.size() || buf[pos] != kBytesMarker)
        return false;
    for (++pos; pos < buf.size();) {
        auto escapePos = buf.find((char) kEscape, pos);
        if (escapePos == std::string::npos || escapePos + 1 >= buf.size()) return false;
        auto escapeChar = (unsigned char) buf[escapePos + 1];
        if (escapeChar == kEscapedTerm) {
            if (out) out->append(buf.substr(pos, escapePos - pos));
            pos = escapePos + 2;
            return true;
        }
        if (escapeChar != kEscaped00) return false;
        if (out) out->append(buf.substr(pos, escapePos - pos + 1));
        pos = escapePos + 2;
    }
    return false;
}

std::string EncodeToHexString(const std::string &str) {
    std::string result;
    char buf[3];
    for (std::string::size_type i = 0; i < str.size(); ++i) {
        snprintf(buf, 3, "%02x", static_cast<unsigned char>(str[i]));
        result.append(buf, 2);
    }
    return result;
}

} /* namespace dataserver */
} /* namespace sharkstore */
