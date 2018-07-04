#include "command_line.h"

#include <unistd.h>
#include "server.h"

using namespace sharkstore::raft::playground;

int main(int argc, char *argv[]) {
    parseCommandLine(argc, argv);
    checkCommandLineValid();

    Server server;
    server.run();
    pause();

    return 0;
}
