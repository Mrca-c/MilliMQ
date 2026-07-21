#pragma once
#include <string>
#include <atomic>
#include <vector>
#include <unordered_map>
#include "index_manager.h"

namespace millimq
{
    class NetworkServer
    {
    public:
        explicit NetworkServer(int port, millimq::IndexManager &engine);
        ~NetworkServer();

        void run();
        void stop();

        struct Connection
        {
            std::vector<char> recv_buf;
            std::vector<char> send_buf;
            bool send_pending = false;
        };

    private:
        int listen_fd_ = -1;
        int epfd_ = -1;
        std::atomic<bool> running_{false};
        std::unordered_map<int, Connection> conns_;

        millimq::IndexManager &engine_;
    };
}