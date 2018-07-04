_Pragma("once");

#include <stdint.h>
#include <string>
#include "base/status.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

static const uint16_t kLogCurrentVersion = 1;
static const char* kLogFileMagic = "\x99\xA3\xB8\xDE";

// 日志文件名格式( {seq-startindex}.log,
// 如0000000000000003-0000000000000012.log,
// 前缀为十六进制的文件序号和起始日志offset)
static const char* kLogFileNameRegex = "([0-9a-f]{16})-([0-9a-f]{16})\\.log";
// 备份文件名的子串，用于识别备份文件
static const char* kLogBackupSubStr = ".log.bak.";

std::string makeLogFileName(uint64_t seq, uint64_t index);
bool parseLogFileName(const std::string& name, uint64_t& seq, uint64_t& index);
bool isBakLogFile(const std::string& name);

// 固定64字节
struct Footer {
    char magic[4] = {'\0'};
    uint16_t version = kLogCurrentVersion;
    uint32_t index_offset = 0;
    char reserved[54] = {'\0'};

    // convert to big-endian when write to file
    void Encode();
    // conver to host-endian when read from file
    void Decode();

    Status Validate() const;

} __attribute__((packed));

enum RecordType : uint8_t { kLogEntry = 1, kIndex };

struct Record {
    RecordType type = kLogEntry;
    uint32_t size = 0;
    uint32_t crc = 0;
    char payload[0];

    // convert to big-endian when write to file
    void Encode();
    // conver to host-endian when read from file
    void Decode();

} __attribute__((packed));

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
