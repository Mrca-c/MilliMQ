#include "network_server.h"
#include <iostream>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <vector>
#include <fcntl.h>

NetworkServer::NetworkServer(int port)
{
    // 1. 创建 TCP socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
        throw std::runtime_error("socket failed");

    // 2. 允许地址重用
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 3. 绑定本地端口
    struct sockaddr_in addr{}; // 结构体清零
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(listen_fd_, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("bind failed");

    // 4. 开始监听，最大等待队列为 10
    if (listen(listen_fd_, 10) < 0)
        throw std::runtime_error("listen failed");

    std::cout << "Server listening on port " << port << std::endl;

    // 创建epoll实例
    epfd_ = epoll_create1(0);
    if (epfd_ < 0)
        throw std::runtime_error("epoll_create1 failed");
    struct epoll_event ev;
    ev.events = EPOLLIN;                                      // 监听读事件
    ev.data.fd = listen_fd_;                                  // 绑定文件描述符
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) // 关心listen_fd的读事件
        throw std::runtime_error("epoll_ctl listen_fd failed");
}

NetworkServer::~NetworkServer()
{
    if (listen_fd_ >= 0)
        close(listen_fd_);

    if (epfd_ >= 0)
        close(epfd_);
}

static bool set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
        return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

void NetworkServer::run()
{
    running_.store(true);  //原子操作
    std::vector<struct epoll_event> events(64);

    while (running_.load())
    {
        int n = epoll_wait(epfd_, events.data(), events.size(), 500);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            break;
        }

        for (int i = 0; i < n; ++i)
        {
            int fd = events[i].data.fd;
            if (fd == listen_fd_)
            {
                while (true)
                {
                    struct sockaddr_in client_addr;
                    socklen_t addr_len = sizeof(client_addr);
                    int client_fd = accept(listen_fd_, (struct sockaddr *)&client_addr, &addr_len);
                    if (client_fd < 0)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        else
                        {
                            std::cerr << "accept error" << std::endl;
                            break;
                        }
                    }
                    set_nonblocking(client_fd);
                    struct epoll_event ev;
                    ev.events = EPOLLIN;
                    ev.data.fd = client_fd;
                    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, client_fd, &ev) < 0)
                    {
                        std::cerr << "epoll_ctl add client failed" << std::endl;
                        close(client_fd);
                    }
                }
            }
            else
            {
                char buf[4096];
                ssize_t n = read(fd, buf, sizeof(buf));
                if (n > 0)
                {
                    write(fd, buf, n);
                }
                else
                {
                    epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            }
        }
    }
}

void NetworkServer::stop()
{
    running_.store(false);
}