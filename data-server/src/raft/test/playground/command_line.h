_Pragma("once");

#include <stdint.h>
#include <vector>

namespace sharkstore {
namespace raft {
namespace playground {

extern uint64_t gNodeID;
extern std::vector<uint64_t> gPeers;

void parseCommandLine(int argc, char *argv[]);
void checkCommandLineValid();

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */
