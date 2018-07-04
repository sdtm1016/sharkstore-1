_Pragma("once");

#include <stdint.h>
#include <string>

namespace sharkstore {
namespace dataserver {
namespace storage {

enum class FieldType : char {
    kInt,
    kUInt,
    kFloat,
    kBytes,
};

class FieldValue {
public:
    explicit FieldValue(int64_t val) : type_(FieldType::kInt) {
        value_.ival = val;
    }
    explicit FieldValue(uint64_t val) : type_(FieldType::kUInt) {
        value_.uval = val;
    }
    explicit FieldValue(double val) : type_(FieldType::kFloat) {
        value_.fval = val;
    }
    explicit FieldValue(std::string* val) : type_(FieldType::kBytes) {
        value_.sval = val;
    }
    explicit FieldValue(const std::string& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(val);
    }

    ~FieldValue() {
        if (FieldType::kBytes == type_) delete value_.sval;
    }

    FieldValue(const FieldValue&) = delete;
    FieldValue& operator=(const FieldValue&) = delete;

    FieldType Type() const { return type_; }

    int64_t Int() const {
        if (type_ != FieldType::kInt) return 0;
        return value_.ival;
    }

    uint64_t UInt() const {
        if (type_ != FieldType::kUInt) return 0;
        return value_.uval;
    }

    double Float() const {
        if (type_ != FieldType::kFloat) return 0;
        return value_.fval;
    }

    const std::string& Bytes() const {
        if (type_ != FieldType::kBytes || value_.sval == nullptr)
            return kDefaultBytes;
        return *value_.sval;
    }

private:
    static const std::string kDefaultBytes;

    FieldType type_;
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
        std::string* sval;
    } value_;
};

enum class CompareOp : char {
    kLess,
    kGreater,
    kEqual,
};

bool fcompare(const FieldValue& lh, const FieldValue& rh, CompareOp op);

FieldValue* CopyValue(const FieldValue& f);
void EncodeFieldValue(std::string* buf, FieldValue* v);

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
