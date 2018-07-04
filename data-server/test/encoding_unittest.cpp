#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <limits>

#include "common/ds_encoding.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace std;
using namespace sharkstore::dataserver;

std::string toHex(const std::string& str) {
    std::string result;
    char buf[3];
    for (std::string::size_type i = 0; i < str.size(); ++i) {
        snprintf(buf, 3, "%02x", static_cast<unsigned char>(str[i]));
        result.append(buf, 2);
    }
    return result;
}

//TEST(Encoding, AA) {
//    std::string buf("\x01\x00\x00\x00\x00\x00\x01\x9c\xe4\x89", 10);
//    size_t pos = 9;
//    uint64_t value = 0;
//    bool ret = DecodeUvarintAscending(buf, pos, &value);
//    ASSERT_TRUE(ret);
//    std::cout << value << std::endl;
//}

TEST(Encoding, NonSortingUVariant) {
    auto testFunc = [](uint64_t num, const std::string& expected_buf) {
        std::string buf;
        EncodeNonSortingUvarint(&buf, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }

        uint64_t value;
        size_t offset = 0;
        ASSERT_TRUE(DecodeNonSortingUvarint(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);
    };

    uint64_t a = 1;
    testFunc(0, "00");
    testFunc(a << 7, "8100");
    testFunc(a << 14, "818000");
    testFunc(a << 21, "81808000");
    testFunc(a << 28, "8180808000");
    testFunc(a << 35, "818080808000");
    testFunc(a << 42, "81808080808000");
    testFunc(a << 49, "8180808080808000");
    testFunc(a << 56, "818080808080808000");
    testFunc(std::numeric_limits<uint64_t>::max(), "81ffffffffffffffff7f");
}

TEST(Encoding, NonSortingVariant) {
    auto testFunc = [](int64_t num, const std::string& expected_buf) {
        std::string buf;
        EncodeNonSortingVarint(&buf, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }

        int64_t value;
        size_t offset = 0;
        ASSERT_TRUE(DecodeNonSortingVarint(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);
    };

    testFunc(0, "00");
    testFunc(1, "02");
    testFunc(-1, "01");
    testFunc(123, "f601");
    testFunc(-123, "f501");
    testFunc(std::numeric_limits<int64_t>::min(), "ffffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::max(), "feffffffffffffffff01");
}

TEST(Encoding, IntValue) {
    auto testFunc = [](int64_t num, uint32_t col_id, const std::string expected_buf) {
        std::string buf;
        EncodeIntValue(&buf, col_id, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }

        int64_t value;
        size_t offset = 0;
        ASSERT_TRUE(DecodeIntValue(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);

        EncodeType type;
        offset = 0;
        uint32_t actual_col_id = 0;
        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
        ASSERT_LE(offset, buf.size());
        ASSERT_EQ(type, EncodeType::Int);
        ASSERT_EQ(actual_col_id, col_id);
    };

    testFunc(0, kNoColumnID, "0300");
    testFunc(123, kNoColumnID, "03f601");
    testFunc(-123, kNoColumnID, "03f501");
    testFunc(std::numeric_limits<int64_t>::max(), kNoColumnID, "03feffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::min(), kNoColumnID, "03ffffffffffffffffff01");

    testFunc(0, 123, "8f3300");
    testFunc(123, 123, "8f33f601");
    testFunc(-123, 123, "8f33f501");
    testFunc(std::numeric_limits<int64_t>::max(), 123, "8f33feffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::min(), 123, "8f33ffffffffffffffffff01");

    uint32_t max_col = std::numeric_limits<uint32_t>::max();
    testFunc(0, max_col, "81ffffffff7300");
    testFunc(123, max_col, "81ffffffff73f601");
    testFunc(-123, max_col, "81ffffffff73f501");
    testFunc(std::numeric_limits<int64_t>::max(), max_col, "81ffffffff73feffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::min(), max_col, "81ffffffff73ffffffffffffffffff01");
}

TEST(Encoding, FloatValue) {
    auto testFunc = [](double num, uint32_t col_id, const std::string expected_buf) {
        std::string buf;
        EncodeFloatValue(&buf, col_id, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }

        double value = 0;
        size_t offset = 0;
        ASSERT_TRUE(DecodeFloatValue(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);

        EncodeType type;
        offset = 0;
        uint32_t actual_col_id = 0;
        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
        ASSERT_LE(offset, buf.size());
        ASSERT_EQ(type, EncodeType::Float);
        ASSERT_EQ(actual_col_id, col_id);
    };

    testFunc(0, kNoColumnID, "040000000000000000");
    testFunc(0.00123, kNoColumnID, "043f5426fe718a86d7");
    testFunc(-0.00123, kNoColumnID, "04bf5426fe718a86d7");
    double val = 1.797693134862315708145274237317043567981e+30;
    testFunc(val, kNoColumnID, "");
}

TEST(Encoding, BytesValue) {
    std::string buf;
    EncodeBytesValue(&buf, 123, "aabbccdd", 8);
    ASSERT_EQ(toHex(buf), "8f36086161626263636464");

    std::string value;
    size_t offset = 0;
    ASSERT_TRUE(DecodeBytesValue(buf, offset, &value));
    ASSERT_EQ(offset, buf.size());
    ASSERT_EQ(value, "aabbccdd");

    EncodeType type;
    offset = 0;
    uint32_t actual_col_id = 0;
    ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
    ASSERT_LE(offset, buf.size());
    ASSERT_EQ(type, EncodeType::Bytes);
    ASSERT_EQ(actual_col_id, 123);
}

TEST(Encoding, AscInt) {
    std::string buf;
    EncodeVarintAscending(&buf, 0);
    ASSERT_EQ(toHex(buf), "88");
    int64_t value = 0;
    size_t offset = 0;
    ASSERT_TRUE(DecodeVarintAscending(buf, offset, &value));
    ASSERT_EQ(value, 0);

    EncodeVarintAscending(&buf, -100);
    ASSERT_TRUE(DecodeVarintAscending(buf, offset, &value));
    ASSERT_EQ(value, -100);

    EncodeVarintAscending(&buf, 1);
    ASSERT_TRUE(DecodeVarintAscending(buf, offset, &value));
    ASSERT_EQ(value, 1);
}

// end namespace
}