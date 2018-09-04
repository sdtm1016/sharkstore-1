#include "table.h"

namespace sharkstore {
namespace test {
namespace helper  {

Table::Table(const std::string& table_name, uint32_t table_id) {
    meta_.set_db_id(1);
    meta_.set_db_name("shark_test_db");
    meta_.set_name(table_name);
    meta_.set_id(table_id);
}

Table::Table(const metapb::Table& meta) : meta_(meta) {
    if (!meta_.columns().empty()) {
        last_col_id_ = meta_.columns(meta_.columns_size() - 1).id();
    }
}

std::vector<metapb::Column> Table::GetPKs() const {
    std::vector<metapb::Column> cols;
    for (const auto& col : meta_.columns()) {
        if (col.primary_key() > 0) cols.push_back(col);
    }
    return cols;
}

std::vector<metapb::Column> Table::GetNonPkColumns() const {
    std::vector<metapb::Column> cols;
    for (const auto& col : meta_.columns()) {
        if (col.primary_key() == 0) cols.push_back(col);
    }
    return cols;
}

std::vector<metapb::Column> Table::GetAllColumns() const {
    std::vector<metapb::Column> cols;
    for (const auto& col : meta_.columns()) {
        cols.push_back(col);
    }
    return cols;
}


metapb::Column Table::GetColumn(uint64_t id) const {
    auto it = std::find_if(meta_.columns().cbegin(), meta_.columns().cend(),
            [id](const metapb::Column& col){ return col.id() == id; });
    if (it != meta_.columns().cend()) {
        return *it;
    } else {
        throw std::runtime_error(std::string("column id not found: ") + std::to_string(id));
    }
}

metapb::Column Table::GetColumn(const std::string& name) const {
    auto it = std::find_if(meta_.columns().cbegin(), meta_.columns().cend(),
                           [&name](const metapb::Column& col){ return col.name() == name; });
    if (it != meta_.columns().cend()) {
        return *it;
    } else {
        throw std::runtime_error(std::string("column name not found: ") + name);
    }
}

void Table::AddColumn(const std::string& name, metapb::DataType type, bool is_pk) {
    auto col = meta_.add_columns();
    col->set_id(++last_col_id_);
    col->set_data_type(type);
    col->set_name(name);
    col->set_primary_key(is_pk ? 1 : 0);
}

std::unique_ptr<Table> CreateAccountTable() {
    std::unique_ptr<Table> t(new Table("account", 1));
    t->AddColumn("id", metapb::BigInt, true);
    t->AddColumn("name", metapb::Varchar);
    t->AddColumn("balance", metapb::BigInt);
    return t;
}

std::unique_ptr<Table> CreateUserTable() {
    std::unique_ptr<Table> t(new Table("user", 2));
    t->AddColumn("user_name", metapb::Varchar, true);
    t->AddColumn("pass_word", metapb::Varchar);
    t->AddColumn("real_name", metapb::Varchar);
    return t;
}

std::unique_ptr<Table> CreateHashUserTable() {
    std::unique_ptr<Table> t(new Table("user", 3));
    t->AddColumn("h", metapb::Int, true);
    t->AddColumn("user_name", metapb::Varchar, true);
    t->AddColumn("pass_word", metapb::Varchar);
    t->AddColumn("real_name", metapb::Varchar);
    return t;
}

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
