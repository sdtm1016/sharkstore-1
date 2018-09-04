_Pragma("once");

namespace sharkstore {
namespace dataserver {
namespace server {

std::string GetGitDescribe();

std::string GetVersionInfo();

std::string GetBuildDate();

std::string GetBuildType();

std::string GetRocksdbVersion();

} /* namespace server */
} /* namespace dataserver */
} /* namespace fbase */
