#include <string>
#include <sstream>
#include <rocksdb/version.h>

#include "version_gen.h"


namespace sharkstore {
namespace dataserver {
namespace server {

std::string GetGitDescribe() {
    return GIT_DESC;
}

std::string GetVersionInfo() {
    std::ostringstream ss;
    ss << "Git Describe:\t" << GIT_DESC << std::endl;
    ss << "Build Date:\t" << BUILD_DATE << std::endl;
    ss << "Build Type:\t" << BUILD_TYPE << std::endl;
    ss << "Build Flags:\t" << BUILD_FLAGS << std::endl;
    ss << "Rocksdb:\t" << ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH;
    return ss.str();
}

} /* namespace server */
} /* namespace dataserver */
} /* namespace sharkstore */
