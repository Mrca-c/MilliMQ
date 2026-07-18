#include "segment_manager.h"
#include "index_manager.h"
#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <sys/stat.h>
#include <thread>

int main()
{
    mkdir("./data", 0755);

    const std::string data_dir = "./data";
    const uint64_t seg_max_size = 1024 * 1024;
    const size_t max_pool_size = 8;

    millimq::SegmentManager sm(data_dir, seg_max_size, max_pool_size);
    millimq::IndexManager im(sm);

    const uint32_t topic_count = 100000;
    std::cout << "Producing " << topic_count << " messages..." << std::endl;
    auto start = std::chrono::steady_clock::now();

    std::vector<char> payload_template(100, 'x');
    for (uint32_t tid = 0; tid < topic_count; ++tid)
    {
        uint64_t seq = im.produce(tid, payload_template.data(), payload_template.size());
        if (seq == UINT64_MAX)
        {
            std::cerr << "Produce failed for topic " << tid << std::endl;
        }
    }

    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Produce done in " << ms << " ms, rate = "
              << (topic_count * 1000.0 / ms) << " msg/s" << std::endl;

    std::cout << "Current pool size (est): " << sm.debug_pool_size() << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dist(0, topic_count - 1);

    size_t consumed = 0;
    for (int i = 0; i < 5000; ++i)
    {
        uint32_t tid = dist(gen);
        size_t n = im.consume(tid, 0, 1,
                              [](uint32_t, uint64_t, const std::vector<char> &) {});
        consumed += n;
    }
    std::cout << "Random consume done, consumed " << consumed << " messages" << std::endl;
    std::cout << "Current pool size: " << sm.debug_pool_size() << std::endl;

    std::cout << "Program will exit in 10 seconds, observe file handles now." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    return 0;
}