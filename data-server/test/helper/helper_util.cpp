#include "helper_util.h"

#include "common/ds_encoding.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace sharkstore::dataserver;

uint64_t GetPeerID(uint64_t node_id) {
    return node_id + 100;
}

metapb::Range MakeRangeMeta(Table *t, size_t peers_num) {
    metapb::Range meta;
    meta.set_id(1);
    EncodeKeyPrefix(meta.mutable_start_key(), t->GetID());
    EncodeKeyPrefix(meta.mutable_end_key(), t->GetID() + 1);

    for (size_t i = 0; i < peers_num; ++i) {
        auto peer = meta.add_peers();
        peer->set_node_id(i + 1);
        peer->set_id(GetPeerID(peer->node_id()));
        peer->set_type(metapb::PeerType_Normal);
    }
    meta.mutable_range_epoch()->set_version(peers_num);
    meta.mutable_range_epoch()->set_conf_ver(peers_num);

    meta.set_table_id(t->GetID());
    auto pks = t->GetPKs();
    if (pks.size() == 0) {
        throw std::runtime_error("invalid table(no primary key)");
    }
    for (const auto& pk : pks) {
        auto p = meta.add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

static const char kKeyPrefixByte = '\x01';

void EncodeKeyPrefix(std::string *buf, uint64_t table_id) {
    buf->push_back(kKeyPrefixByte);
    EncodeUint64Ascending(buf, table_id);
}

// append encoded pk values to buf
void EncodePrimaryKey(std::string *buf, const metapb::Column& col, const std::string& val) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(val.c_str(), NULL, 10);
                EncodeVarintAscending(buf, i);
            } else {
                uint64_t i = strtoull(val.c_str(), NULL, 10);
                EncodeUvarintAscending(buf, i);
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(val.c_str(), NULL);
            EncodeFloatAscending(buf, d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            EncodeBytesAscending(buf, val.c_str(), val.size());
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodePrimaryKey: invalid column data type: ") +
                std::to_string(static_cast<int>(col.data_type())));
    }
}

void EncodeColumnValue(std::string *buf, const metapb::Column& col, const std::string& val) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(val.c_str(), NULL, 10);
                EncodeIntValue(buf, static_cast<uint32_t>(col.id()), i);
            } else {
                uint64_t i = strtoull(val.c_str(), NULL, 10);
                EncodeIntValue(buf, static_cast<uint32_t>(col.id()), static_cast<int64_t>(i));
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(val.c_str(), NULL);
            EncodeFloatValue(buf, static_cast<uint32_t>(col.id()), d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            EncodeBytesValue(buf, static_cast<uint32_t>(col.id()), val.c_str(), val.size());
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                     std::to_string(static_cast<int>(col.data_type())));
    }
}

void DecodeColumnValue(const std::string& buf, size_t& offset, const metapb::Column& col, std::string *val) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            int64_t i = 0;
            DecodeIntValue(buf, offset, &i);
            if (col.unsigned_()) {
                *val = std::to_string(static_cast<uint64_t>(i));
            } else {
                *val = std::to_string(i);
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = 0.0;
            DecodeFloatValue(buf, offset, &d);
            *val = std::to_string(d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            DecodeBytesValue(buf, offset, val);
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                     std::to_string(static_cast<int>(col.data_type())));
    }
}

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */

