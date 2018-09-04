#include "query_parser.h"

#include <sstream>
#include "helper_util.h"

namespace sharkstore {
namespace test {
namespace helper {

SelectResultParser::SelectResultParser(const kvrpcpb::SelectRequest& req,
                                       const kvrpcpb::SelectResponse& resp) {
    metapb::Column fake_count_col;
    fake_count_col.set_data_type(metapb::BigInt);

    rows_.reserve(resp.rows_size());
    for (const auto& r: resp.rows()) {
        keys_.push_back(r.key());

        std::vector<std::string> values;
        size_t offset = 0;
        for (const auto& f: req.field_list()) {
            std::string val;
            if (f.typ() == kvrpcpb::SelectField_Type_Column) {
                DecodeColumnValue(r.fields(), offset, f.column(), &val);
            } else {
                if (f.aggre_func() == "count") {
                    DecodeColumnValue(r.fields(), offset, fake_count_col, &val);
                } else {
                    DecodeColumnValue(r.fields(), offset, f.column(), &val);
                }
            }
            values.push_back(std::move(val));
        }
        rows_.push_back(std::move(values));
    }
}

static std::string ToDebugString(const std::vector<std::vector<std::string>>& rows) {
    std::ostringstream ss;
    ss << "[\n" ;
    for (const auto& row: rows) {
        ss << " { ";
        for (size_t i = 0; i < row.size(); ++i) {
            ss << row[i];
            if (i != row.size() - 1) {
                ss << ", ";
            }
        }
        ss << " }\n";
    }
    ss << "]" ;
    auto s = ss.str();
    return s;
}

Status SelectResultParser::Match(
        const std::vector<std::vector<std::string>>& expected_rows) const {
    bool matched = true;
    do {
        if (expected_rows.size() != rows_.size()) {
            matched = false;
            break;
        }

        for (size_t i = 0; i < rows_.size(); ++i) {
            if (rows_[i] != expected_rows[i]) {
                matched = false;
                break;
            }
        }
    } while(0);

    if (matched) {
        return Status::OK();
    } else {
        std::ostringstream ss;
        ss << "\nexpected: \n";
        ss <<  ToDebugString(expected_rows);
        ss << "\nactual: \n";
        ss << ToDebugString(rows_);
        return Status(Status::kUnknown, "mismatch", ss.str());
    }
}

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
