#include <iostream>
#include <cstdlib>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <endian.h>
#include <chrono>
#include <algorithm>

static std::vector<char> make_frame(const char *payload, size_t len)
{
    std::vector<char> frame(4 + len);
    uint32_t net_len = htonl(static_cast<uint32_t>(len));
    memcpy(frame.data(), &net_len, 4);
    memcpy(frame.data() + 4, payload, len);
    return frame;
}

static bool read_exact(int fd, void *buf, size_t n)
{
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, (char *)buf + total, n - total);
        if (r <= 0) return false;
        total += r;
    }
    return true;
}

int main(int argc, char **argv)
{
    int total = 50000;
    int batch = 2000;
    if (argc > 1) total = std::atoi(argv[1]);
    if (argc > 2) batch = std::atoi(argv[2]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9999);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    uint32_t topic = 1;
    std::vector<char> base(100, 'x');

    std::vector<char> req;
    req.push_back(0x01);
    uint32_t net_topic = htonl(topic);
    req.insert(req.end(), (char *)&net_topic, (char *)&net_topic + 4);
    uint32_t net_len = htonl(100);
    req.insert(req.end(), (char *)&net_len, (char *)&net_len + 4);
    req.insert(req.end(), base.begin(), base.end());

    auto frame = make_frame(req.data(), req.size());

    long ok = 0, err = 0, sent = 0;
    auto t1 = std::chrono::steady_clock::now();

    while (sent < total) {
        long n = std::min((long)batch, (long)total - sent);

        for (long i = 0; i < n; ++i)
            send(sock, frame.data(), frame.size(), 0);

        for (long i = 0; i < n; ++i) {
            uint32_t resp_len;
            if (!read_exact(sock, &resp_len, 4)) {
                std::cerr << "recv header error at " << (sent + i) << std::endl;
                goto done;
            }
            resp_len = ntohl(resp_len);
            std::vector<char> payload(resp_len);
            if (!read_exact(sock, payload.data(), resp_len)) {
                std::cerr << "recv payload error at " << (sent + i) << std::endl;
                goto done;
            }
            if (payload.size() >= 2 && payload[0] == 0x00) ++ok;
            else ++err;
        }

        sent += n;
        if (sent % 100000 == 0 || sent == total)
            std::cerr << "  progress: " << sent << "/" << total << std::endl;
    }

done:
    auto t2 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t2 - t1).count();

    std::cout << "Total:      " << total << std::endl;
    std::cout << "Success:    " << ok << std::endl;
    std::cout << "Errors:     " << err << std::endl;
    std::cout << "Duration:   " << sec << " s" << std::endl;
    std::cout << "Throughput: " << (ok / sec) << " msg/s" << std::endl;

    close(sock);
    return (ok == total) ? 0 : 1;
}
