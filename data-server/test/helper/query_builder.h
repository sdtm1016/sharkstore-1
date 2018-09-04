_Pragma("once");

#include "proto/gen/kvrpcpb.pb.h"

#include "table.h"

namespace sharkstore {
namespace test {
namespace helper {

class SelectRequestBuilder {
public:
    explicit SelectRequestBuilder(Table *t);

    // select one row
    void SetKey(const std::vector<std::string>& all_pk_values);

    // select multi rows
    void SetScope(const std::vector<std::string>& start_pk_values,
            const std::vector<std::string>& end_pk_values);

    // select field list
    void AddField(const std::string& col_name);
    void AddAllFields(); // select *
    std::vector<metapb::Column> AddRandomFields(size_t size = 0);
    void AddAggreFunc(const std::string& func_name, const std::string& col_name);

    // select where filter
    void AddMatch(const std::string& col, kvrpcpb::MatchType type, const std::string& val);

    // select limit
    void AddLimit(uint64_t count, uint64_t offset = 0);

    kvrpcpb::SelectRequest Build() { return std::move(req_); }

private:
    Table *table_ = nullptr;
    kvrpcpb::SelectRequest req_;
};


class DeleteRequestBuilder {
public:
    explicit DeleteRequestBuilder(Table *t);

    // delete one row
    void SetKey(const std::vector<std::string>& all_pk_values);

    // detete multi rows
    void SetScope(const std::vector<std::string>& start_pk_values,
                  const std::vector<std::string>& end_pk_values);

    // select where filter
    void AddMatch(const std::string& col, kvrpcpb::MatchType type, const std::string& val);

    kvrpcpb::DeleteRequest Build() { return std::move(req_); }

private:
    Table *table_ = nullptr;
    kvrpcpb::DeleteRequest req_;
};


class InsertRequestBuilder {
public:
    explicit InsertRequestBuilder(Table *t);

    void AddRow(const std::vector<std::string>& values);
    void AddRows(const std::vector<std::vector<std::string>>& rows);
    void SetCheckDuplicate();

    kvrpcpb::InsertRequest Build() { return std::move(req_); }

private:
    Table *table_ = nullptr;
    std::vector<metapb::Column> pk_columns_;
    std::vector<metapb::Column> non_pk_columns_;
    kvrpcpb::InsertRequest req_;
};

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
