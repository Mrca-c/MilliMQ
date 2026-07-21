#include "segment_manager.h"
#include "index_manager.h"
#include "network_server.h"
#include <iostream>
#include <csignal>
#include <sys/stat.h>

millimq::NetworkServer *g_server = nullptr;

void signal_handler(int)
{
    if (g_server)
        g_server->stop();
}

int main()
{
    mkdir("./data", 0755);

    millimq::SegmentManager sm("./data", 1024 * 1024, 8);
    millimq::IndexManager im(sm);
    millimq::NetworkServer server(9999, im);
    g_server = &server;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    std::cout << "MilliMQ storage engine server running on port 9999..." << std::endl;
    server.run();
    std::cout << "Server shut down." << std::endl;
    return 0;
}