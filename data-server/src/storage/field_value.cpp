#include "field_value.h"

#include <assert.h>
#include "common/ds_encoding.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

const std::string FieldValue::kDefaultBytes;

bool compareInt(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Int() < rh.Int();
        case CompareOp::kEqual:
            return lh.Int() == rh.Int();
        case CompareOp::kGreater:
            return lh.Int() > rh.Int();
        default:
            return false;
    }
}

bool compareUInt(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.UInt() < rh.UInt();
        case CompareOp::kEqual:
            return lh.UInt() == rh.UInt();
        case CompareOp::kGreater:
            return lh.UInt() > rh.UInt();
        default:
            return false;
    }
}

bool compareFloat(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Float() < rh.Float();
        case CompareOp::kEqual:
            return lh.Float() == rh.Float();
        case CompareOp::kGreater:
            return lh.Float() > rh.Float();
        default:
            return false;
    }
}

bool compareBytes(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    switch (op) {
        case CompareOp::kLess:
            return lh.Bytes() < rh.Bytes();
        case CompareOp::kEqual:
            return lh.Bytes() == rh.Bytes();
        case CompareOp::kGreater:
            return lh.Bytes() > rh.Bytes();
        default:
            return false;
    }
}

bool fcompare(const FieldValue& lh, const FieldValue& rh, CompareOp op) {
    if (lh.Type() != rh.Type()) {
        return false;
    }

    switch (lh.Type()) {
        case FieldType::kInt:
            return compareInt(lh, rh, op);
        case FieldType::kUInt:
            return compareUInt(lh, rh, op);
        case FieldType::kFloat:
            return compareFloat(lh, rh, op);
        case FieldType::kBytes:
            return compareBytes(lh, rh, op);
        default:
            return false;
    }
}

FieldValue* CopyValue(const FieldValue& v) {
    switch (v.Type()) {
        case FieldType::kInt:
            return new FieldValue(v.Int());
        case FieldType::kUInt:
            return new FieldValue(v.UInt());
        case FieldType::kFloat:
            return new FieldValue(v.Float());
        case FieldType::kBytes:
            return new FieldValue(v.Bytes());
    }
    return nullptr;
}

void EncodeFieldValue(std::string* buf, FieldValue* v) {
    if (v == nullptr) {
        EncodeNullValue(buf, kNoColumnID);
        return;
    }

    switch (v->Type()) {
        case FieldType::kInt:
            EncodeIntValue(buf, kNoColumnID, v->Int());
            return;
        case FieldType::kUInt:
            EncodeIntValue(buf, kNoColumnID, static_cast<int64_t>(v->UInt()));
            break;
        case FieldType::kFloat:
            EncodeFloatValue(buf, kNoColumnID, v->Float());
            break;
        case FieldType::kBytes:
            EncodeBytesValue(buf, kNoColumnID, v->Bytes().c_str(), v->Bytes().size());
            break;
    }
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */