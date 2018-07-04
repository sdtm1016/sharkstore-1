_Pragma("once");

#include "field_value.h"
#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class AggreCalculator {
public:
    AggreCalculator(const metapb::Column* col) : col_(col) {}
    virtual ~AggreCalculator() {}

    // NOTE: 函数可能会更改f指针值, 调用完后不可继续使用f
    virtual void Add(const FieldValue* f) = 0;

    virtual int64_t Count() const = 0;

    virtual std::unique_ptr<FieldValue> Result() = 0;

    static std::unique_ptr<AggreCalculator> New(const std::string& name,
                                                const metapb::Column* col);

protected:
    const metapb::Column* const col_;
};

class CountCalculator : public AggreCalculator {
public:
    CountCalculator(const metapb::Column* col);
    ~CountCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    int64_t count_ = 0;
};

class MinCalculator : public AggreCalculator {
public:
    MinCalculator(const metapb::Column* col);
    ~MinCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    FieldValue* min_value_ = nullptr;
};

class MaxCalculator : public AggreCalculator {
public:
    MaxCalculator(const metapb::Column* col);
    ~MaxCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    FieldValue* max_value_ = nullptr;
};

class SumCalculator : public AggreCalculator {
public:
    SumCalculator(const metapb::Column* col);
    ~SumCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    int64_t count_ = 0;
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
    } sum_;
    FieldType type_ = FieldType::kBytes;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
