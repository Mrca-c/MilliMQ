#pragma once

#include <string>
#include <atomic>

class NetworkServer
{
public:
    explicit NetworkServer(int port);
    ~NetworkServer();

    void run();
    void stop();
    int get_fd() const { return listen_fd_; };

private:
    int listen_fd_ = -1;
    int epfd_ = -1;
    std::atomic<bool> running_{false};
};