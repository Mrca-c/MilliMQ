#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <endian.h>

// 简单的帧发送/接收函数（阻塞模式，用于测试）
static std::vector<char> make_frame(const char *payload, size_t len)
{
    std::vector<char> frame(4 + len);
    uint32_t net_len = htonl(len);
    memcpy(frame.data(), &net_len, 4);
    memcpy(frame.data() + 4, payload, len);
    return frame;
}

static bool recv_frame(int fd, std::vector<char> &out_payload)
{
    uint32_t net_len;
    ssize_t n = read(fd, &net_len, 4);
    if (n != 4)
        return false;
    uint32_t len = ntohl(net_len);
    out_payload.resize(len);
    size_t total = 0;
    while (total < len)
    {
        n = read(fd, out_payload.data() + total, len - total);
        if (n <= 0)
            return false;
        total += n;
    }
    return true;
}

int main()
{
    // ---- 1. 生产消息（独立连接） ----
    int prod_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (prod_sock < 0)
    {
        perror("socket");
        return 1;
    }
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9999);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(prod_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("connect failed");
        close(prod_sock);
        return 1;
    }

    uint32_t topic = 1;
    const char *msg = "Hello MilliMQ!";
    uint32_t msglen = strlen(msg);

    // 构建生产请求
    std::vector<char> prod_payload;
    prod_payload.push_back(0x01); // Produce 命令
    uint32_t net_topic = htonl(topic);
    prod_payload.insert(prod_payload.end(), (char *)&net_topic, (char *)&net_topic + 4);
    uint32_t net_msglen = htonl(msglen);
    prod_payload.insert(prod_payload.end(), (char *)&net_msglen, (char *)&net_msglen + 4);
    prod_payload.insert(prod_payload.end(), msg, msg + msglen);

    auto frame = make_frame(prod_payload.data(), prod_payload.size());
    if (send(prod_sock, frame.data(), frame.size(), 0) != (ssize_t)frame.size())
    {
        perror("send");
        close(prod_sock);
        return 1;
    }
    std::vector<char> resp;
    if (!recv_frame(prod_sock, resp))
    {
        std::cerr << "recv produce response failed\n";
        close(prod_sock);
        return 1;
    }
    if (resp.size() < 2)
    {
        std::cerr << "invalid response\n";
        close(prod_sock);
        return 1;
    }
    if (resp[0] == 0x00)
    {
        uint64_t seq = be64toh(*(uint64_t *)(resp.data() + 1));
        std::cout << "Produce success, seq = " << seq << std::endl;
    }
    else
    {
        std::cerr << "Produce error (status=" << (int)resp[0] << ")\n";
    }
    close(prod_sock);

    // ---- 2. 消费消息（独立连接） ----
    int cons_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (cons_sock < 0)
    {
        perror("socket");
        return 1;
    }
    if (connect(cons_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("connect failed");
        close(cons_sock);
        return 1;
    }

    uint64_t start_seq = 0;
    uint32_t max_count = 10;
    std::vector<char> cons_payload;
    cons_payload.push_back(0x02); // Consume 命令
    net_topic = htonl(topic);
    cons_payload.insert(cons_payload.end(), (char *)&net_topic, (char *)&net_topic + 4);
    uint64_t net_start = htobe64(start_seq);
    cons_payload.insert(cons_payload.end(), (char *)&net_start, (char *)&net_start + 8);
    uint32_t net_max = htonl(max_count);
    cons_payload.insert(cons_payload.end(), (char *)&net_max, (char *)&net_max + 4);

    frame = make_frame(cons_payload.data(), cons_payload.size());
    if (send(cons_sock, frame.data(), frame.size(), 0) != (ssize_t)frame.size())
    {
        perror("send");
        close(cons_sock);
        return 1;
    }
    resp.clear();
    if (!recv_frame(cons_sock, resp))
    {
        std::cerr << "recv consume response failed\n";
        close(cons_sock);
        return 1;
    }
    if (resp.size() < 5)
    {
        std::cerr << "invalid response\n";
        close(cons_sock);
        return 1;
    }
    if (resp[0] == 0x00)
    {
        uint32_t msg_count = ntohl(*(uint32_t *)(resp.data() + 1));
        std::cout << "Consume success, message count = " << msg_count << std::endl;
        const char *p = resp.data() + 5;
        for (uint32_t i = 0; i < msg_count; ++i)
        {
            if (p + 4 + 8 + 4 > resp.data() + resp.size())
                break;
            uint32_t t = ntohl(*(uint32_t *)p);
            p += 4;
            uint64_t sq = be64toh(*(uint64_t *)p);
            p += 8;
            uint32_t mlen = ntohl(*(uint32_t *)p);
            p += 4;
            std::string m(p, p + mlen);
            p += mlen;
            std::cout << "  [" << i << "] topic=" << t << " seq=" << sq << " msg=" << m << std::endl;
        }
    }
    else
    {
        std::cerr << "Consume error (status=" << (int)resp[0] << ")\n";
    }
    close(cons_sock);

    return 0;
}