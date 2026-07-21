#include "network_server.h"
#include "protocol.h"
#include <iostream>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <cstring>

namespace millimq
{
    NetworkServer::NetworkServer(int port, millimq::IndexManager &engine)
        : engine_(engine)
    {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0)
            throw std::runtime_error("socket failed");

        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        if (bind(listen_fd_, (struct sockaddr *)&addr, sizeof(addr)) < 0)
            throw std::runtime_error("bind failed");

        if (listen(listen_fd_, 10) < 0)
            throw std::runtime_error("listen failed");

        std::cout << "Server listening on port " << port << std::endl;

        epfd_ = epoll_create1(0);
        if (epfd_ < 0)
            throw std::runtime_error("epoll_create1 failed");

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0)
            throw std::runtime_error("epoll_ctl listen_fd failed");
    }

    NetworkServer::~NetworkServer()
    {
        for (auto &kv : conns_)
        {
            epoll_ctl(epfd_, EPOLL_CTL_DEL, kv.first, nullptr);
            close(kv.first);
        }
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

    static bool try_send(int epfd, int fd, NetworkServer::Connection &conn)
    {
        while (!conn.send_buf.empty())
        {
            ssize_t n = write(fd, conn.send_buf.data(), conn.send_buf.size());
            if (n > 0)
            {
                conn.send_buf.erase(conn.send_buf.begin(), conn.send_buf.begin() + n);
            }
            else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                return false;
            }
            else
            {
                return false;
            }
        }
        conn.send_pending = false;
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        return true;
    }

    static void process_received_data(int epfd, int fd,
                                      std::unordered_map<int, NetworkServer::Connection> &conns,
                                      NetworkServer::Connection &conn,
                                      millimq::IndexManager &engine)
    {

        std::vector<char> frame_data;
        while (millimq::protocol::extract_frame(conn.recv_buf, frame_data))
        {
            if (frame_data.empty())
                continue;
            uint8_t cmd = frame_data[0];
            std::vector<char> response_frame;

            switch (static_cast<millimq::Command>(cmd))
            {
            case millimq::Command::Produce:
            {
                millimq::protocol::ProduceRequest req;
                if (millimq::protocol::parse_produce(frame_data.data(), frame_data.size(), req))
                {
                    uint64_t seq = engine.produce(req.topic_id, req.payload.data(), req.payload.size());
                    if (seq != UINT64_MAX)
                        response_frame = millimq::protocol::build_produce_response(seq);
                    else
                        response_frame = millimq::protocol::build_error_response();
                }
                else
                {
                    response_frame = millimq::protocol::build_error_response();
                }
                break;
            }
            case millimq::Command::Consume:
            {
                millimq::protocol::ConsumeRequest req;
                if (millimq::protocol::parse_consume(frame_data.data(), frame_data.size(), req))
                {
                    std::vector<std::tuple<uint32_t, uint64_t, std::vector<char>>> msgs;
                    engine.consume(req.topic_id, req.start_seq, req.max_count,
                                   [&](uint32_t topic, uint64_t seq, const std::vector<char> &payload)
                                   {
                                       msgs.emplace_back(topic, seq, payload);
                                   });
                    response_frame = millimq::protocol::build_consume_response(msgs);
                }
                else
                {
                    response_frame = millimq::protocol::build_error_response();
                }
                break;
            }
            case millimq::Command::Commit:
            {
                millimq::protocol::CommitRequest req;
                if (millimq::protocol::parse_commit(frame_data.data(), frame_data.size(), req))
                {
                    engine.commit_offset("default", req.topic_id, req.next_seq);
                    response_frame = millimq::protocol::build_commit_response(true);
                }
                else
                {
                    response_frame = millimq::protocol::build_error_response();
                }
                break;
            }
            case millimq::Command::GetOffset:
            {
                millimq::protocol::GetOffsetRequest req;
                if (millimq::protocol::parse_get_offset(frame_data.data(), frame_data.size(), req))
                {
                    uint64_t offset = engine.get_offset("default", req.topic_id);
                    response_frame = millimq::protocol::build_get_offset_response(offset);
                }
                else
                {
                    response_frame = millimq::protocol::build_error_response();
                }
                break;
            }
            default:
                response_frame = millimq::protocol::build_error_response();
                break;
            }

            if (!response_frame.empty())
            {
                conn.send_buf.insert(conn.send_buf.end(), response_frame.begin(), response_frame.end());
                conn.send_pending = true;
            }
        }

        if (conn.send_pending)
        {
            bool sent = try_send(epfd, fd, conn);
            if (!sent && conn.send_buf.empty())
            {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                conns.erase(fd);
                return;
            }
            if (conn.send_pending)
            {
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = fd;
                epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
            }
        }
    }

    void NetworkServer::run()
    {
        running_.store(true);
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
                uint32_t ev_mask = events[i].events;
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
                        conns_[client_fd] = Connection{};
                        auto &conn = conns_[client_fd];

                        char prebuf[4096];
                        ssize_t pren = read(client_fd, prebuf, sizeof(prebuf));
                        if (pren > 0)
                        {
                            conn.recv_buf.insert(conn.recv_buf.end(), prebuf, prebuf + pren);
                            process_received_data(epfd_, client_fd, conns_, conn, engine_);
                            if (conns_.find(client_fd) == conns_.end())
                                continue;
                        }

                        struct epoll_event ev;
                        ev.events = EPOLLIN | (conn.send_pending ? EPOLLOUT : 0);
                        ev.data.fd = client_fd;
                        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, client_fd, &ev) < 0)
                        {
                            std::cerr << "epoll_ctl add client failed" << std::endl;
                            conns_.erase(client_fd);
                            close(client_fd);
                        }
                    }
                }
                else
                {
                    auto it = conns_.find(fd);
                    if (it == conns_.end())
                    {
                        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        continue;
                    }
                    Connection &conn = it->second;

                    if (ev_mask & EPOLLIN)
                    {
                        char buf[4096];
                        ssize_t rn = read(fd, buf, sizeof(buf));
                        if (rn > 0)
                        {
                            conn.recv_buf.insert(conn.recv_buf.end(), buf, buf + rn);
                            process_received_data(epfd_, fd, conns_, conn, engine_);
                            if (conns_.find(fd) == conns_.end())
                                continue;
                        }
                        else if (rn == 0)
                        {
                            epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                            close(fd);
                            conns_.erase(it);
                            continue;
                        }
                        else if (errno != EAGAIN && errno != EWOULDBLOCK)
                        {
                            std::cerr << "read error" << std::endl;
                            epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                            close(fd);
                            conns_.erase(it);
                            continue;
                        }
                    }

                    if (ev_mask & EPOLLOUT)
                    {
                        if (conn.send_pending)
                        {
                            try_send(epfd_, fd, conn);
                        }
                    }

                    if (ev_mask & (EPOLLERR | EPOLLHUP))
                    {
                        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        conns_.erase(it);
                    }
                }
            }
        }
    }

    void NetworkServer::stop()
    {
        running_.store(false);
    }
}