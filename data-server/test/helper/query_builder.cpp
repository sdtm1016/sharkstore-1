#include "query_builder.h"

#include "base/util.h"
#include "helper_util.h"

namespace sharkstore {
namespace test {
namespace helper {

using sharkstore::randomInt;

static std::string buildKey(Table *table, const std::vector<std::string>& values) {
    auto pks = table->GetPKs();
    if (values.size() != pks.size()) {
        throw std::runtime_error("mismatched table primary keys count and input values count");
    }

    std::string buf;
    EncodeKeyPrefix(&buf, table->GetID());
    for (size_t i = 0; i < pks.size(); ++i) {
        EncodePrimaryKey(&buf, pks[i], values[i]);
    }
    return buf;
}

static std::pair<std::string, std::string> buildScope(Table *table,
                                                      const std::vector<std::string>& start_values,
                                                      const std::vector<std::string>& end_values) {
    auto pks = table->GetPKs();
    std::string start, end;

    EncodeKeyPrefix(&start, table->GetID());
    auto len = std::min(pks.size(), start_values.size());
    for (size_t i = 0; i < len; ++i) {
        EncodePrimaryKey(&start, pks[i], start_values[i]);
    }

    if (end_values.empty()) {
        EncodeKeyPrefix(&end, table->GetID() + 1);
    } else {
        EncodeKeyPrefix(&end, table->GetID());
        auto len = std::min(pks.size(), end_values.size());
        for (size_t i = 0; i < len; ++i) {
            EncodePrimaryKey(&end, pks[i], end_values[i]);
        }
    }

    return std::make_pair(start, end);
}


SelectRequestBuilder::SelectRequestBuilder(Table *t) : table_(t) {
    // default select all scope
    SetScope({}, {});
}

void SelectRequestBuilder::SetKey(const std::vector<std::string>& all_pk_values) {
    req_.set_key(buildKey(table_, all_pk_values));
}

void SelectRequestBuilder::SetScope(const std::vector<std::string>& start_pk_values,
              const std::vector<std::string>& end_pk_values) {
    auto ret = buildScope(table_, start_pk_values, end_pk_values);
    req_.mutable_scope()->mutable_start()->assign(ret.first);
    req_.mutable_scope()->mutable_limit()->assign(ret.second);
}

void SelectRequestBuilder::AddField(const std::string& col_name) {
    auto f = req_.add_field_list();
    f->set_typ(kvrpcpb::SelectField_Type_Column);
    f->mutable_column()->CopyFrom(table_->GetColumn(col_name));
}

void SelectRequestBuilder::AddAllFields() {
    auto cols = table_->GetAllColumns();
    for (const auto& col: cols) {
        auto f = req_.add_field_list();
        f->set_typ(kvrpcpb::SelectField_Type_Column);
        f->mutable_column()->CopyFrom(col);
    }
}

std::vector<metapb::Column> SelectRequestBuilder::AddRandomFields(size_t size) {
    std::vector<metapb::Column> field_lists;
    auto cols = table_->GetAllColumns();
    if (size == 0) {
        size = 1 + (sharkstore::randomInt() % cols.size()) * 2;
    }
    for (size_t i = 0; i < size; ++i) {
        auto idx = sharkstore::randomInt() % cols.size();
        auto f = req_.add_field_list();
        f->set_typ(kvrpcpb::SelectField_Type_Column);
        f->mutable_column()->CopyFrom(cols[idx]);
        field_lists.push_back(cols[idx]);
    }
    return field_lists;
}

void SelectRequestBuilder::AddAggreFunc(const std::string& func_name, const std::string& col_name) {
    auto f = req_.add_field_list();
    f->set_typ(kvrpcpb::SelectField_Type_AggreFunction);
    f->mutable_aggre_func()->assign(func_name);
    if (!col_name.empty()) {
        f->mutable_column()->CopyFrom(table_->GetColumn(col_name));
    }
}

void SelectRequestBuilder::AddMatch(const std::string& col, kvrpcpb::MatchType type, const std::string& val) {
    auto w = req_.add_where_filters();
    w->set_match_type(type);
    w->mutable_column()->CopyFrom(table_->GetColumn(col));
    w->mutable_threshold()->assign(val);
}

void SelectRequestBuilder::AddLimit(uint64_t count, uint64_t offset) {
    req_.mutable_limit()->set_count(count);
    req_.mutable_limit()->set_offset(offset);
}


DeleteRequestBuilder::DeleteRequestBuilder(Table *t) : table_(t) {
    // default: delete all scope
    SetScope({}, {});
}

// delete one row
void DeleteRequestBuilder::SetKey(const std::vector<std::string>& all_pk_values) {
    req_.set_key(buildKey(table_, all_pk_values));
}

// detete multi rows
void DeleteRequestBuilder::SetScope(const std::vector<std::string>& start_pk_values,
                                    const std::vector<std::string>& end_pk_values) {
    auto ret = buildScope(table_, start_pk_values, end_pk_values);
    req_.mutable_scope()->mutable_start()->assign(ret.first);
    req_.mutable_scope()->mutable_limit()->assign(ret.second);
}

// select where filter
void DeleteRequestBuilder::AddMatch(const std::string& col,
                                    kvrpcpb::MatchType type,
                                    const std::string& val) {
    auto w = req_.add_where_filters();
    w->set_match_type(type);
    w->mutable_column()->CopyFrom(table_->GetColumn(col));
    w->mutable_threshold()->assign(val);
}



InsertRequestBuilder::InsertRequestBuilder(Table *t) : table_(t) {
    pk_columns_ = t->GetPKs();
    non_pk_columns_ = t->GetNonPkColumns();
}

void InsertRequestBuilder::AddRow(const std::vector<std::string>& values) {
    if (values.size() != pk_columns_.size() + non_pk_columns_.size()) {
        throw std::runtime_error("mismatched row values size with table columns size");
    }

    std::string key;
    std::string value;
    size_t index = 0;

    // encode key
    EncodeKeyPrefix(&key, table_->GetID());
    for (const auto &pk : pk_columns_) {
        EncodePrimaryKey(&key, pk, values[index++]);
    }

    // encode value
    for (const auto &col : non_pk_columns_) {
       EncodeColumnValue(&value, col, values[index++]);
    }
    assert(index == values.size());

    auto kv = req_.add_rows();
    kv->set_key(std::move(key));
    kv->set_value(std::move(value));
}

void InsertRequestBuilder::AddRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row: rows) {
        AddRow(row);
    }
}

void InsertRequestBuilder::SetCheckDuplicate() {
    req_.set_check_duplicate(true);
}


} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
