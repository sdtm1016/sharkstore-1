_Pragma("once");

#include <memory>

#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace test {
namespace helper {

class Table {
public:
    Table(const std::string& table_name, uint32_t table_id);
    explicit Table(const metapb::Table& meta);
    ~Table() = default;

    uint64_t GetID() const { return meta_.id(); }
    std::string GetName() const { return meta_.name(); }

    std::vector<metapb::Column> GetPKs() const;
    std::vector<metapb::Column> GetAllColumns() const;
    std::vector<metapb::Column> GetNonPkColumns() const;
    metapb::Column GetColumn(uint64_t id) const;
    metapb::Column GetColumn(const std::string& name) const;

    const metapb::Table& GetMeta() const { return meta_; }

    void AddColumn(const std::string& name, metapb::DataType type, bool is_pk = false);

private:
    metapb::Table meta_;
    uint64_t last_col_id_ = 0;
};

std::unique_ptr<Table> CreateAccountTable();
std::unique_ptr<Table> CreateUserTable();
std::unique_ptr<Table> CreateHashUserTable();

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
