#include "segment_manager.h"
#include "index_manager.h"
#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <sys/stat.h>
#include <thread>
#include <cstring>
#include <cstdlib>

int main(int argc, char **argv)
{
    mkdir("./data", 0755);

    uint32_t topic_count = 100000;
    if (argc > 1) topic_count = std::atoi(argv[1]);

    const std::string data_dir = "./data";
    const uint64_t seg_max_size = 1024 * 1024;
    const size_t max_pool_size = 8;

    millimq::SegmentManager sm(data_dir, seg_max_size, max_pool_size);
    millimq::IndexManager im(sm);

    std::cout << "Producing " << topic_count << " messages..." << std::endl;
    auto start = std::chrono::steady_clock::now();

    std::vector<char> payload_template(100, 'x');
    for (uint32_t tid = 0; tid < topic_count; ++tid)
    {
        im.produce(tid, payload_template.data(), payload_template.size());
    }

    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Produce done in " << ms << " ms, rate = "
              << (topic_count * 1000.0 / ms) << " msg/s" << std::endl;

    std::cout << "Current pool size (est): " << sm.debug_pool_size() << std::endl;

    // 随机消费 5000 条
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dist(0, topic_count - 1);

    size_t consumed = 0;
    for (int i = 0; i < 5000; ++i)
    {
        uint32_t tid = dist(gen);
        consumed += im.consume(tid, 0, 1,
                               [](uint32_t, uint64_t, const std::vector<char> &) {});
    }
    std::cout << "Random consume done, consumed " << consumed << " messages" << std::endl;
    std::cout << "Current pool size: " << sm.debug_pool_size() << std::endl;

    // 偏移提交测试
    const std::string group = "default";
    uint32_t test_topic = topic_count / 2;

    im.produce(test_topic, "offset_test_msg", 15);

    uint64_t start_seq = im.get_offset(group, test_topic);
    std::cout << "Initial offset for topic " << test_topic << ": " << start_seq << std::endl;

    size_t msg_count = im.consume(test_topic, start_seq, 10,
                                  [](uint32_t tid, uint64_t seq, const std::vector<char> &payload)
                                  {
                                      std::string msg(payload.begin(), payload.end());
                                      std::cout << "  [Group default] Consumed: topic=" << tid
                                                << " seq=" << seq << " msg=" << msg << std::endl;
                                  });

    if (msg_count > 0)
    {
        uint64_t new_offset = start_seq + msg_count;
        im.commit_offset(group, test_topic, new_offset);
        std::cout << "Committed offset to " << new_offset << std::endl;
    }

    uint64_t recovered = im.get_offset(group, test_topic);
    std::cout << "Recovered offset: " << recovered
              << " (expecting " << (start_seq + msg_count) << ")" << std::endl;

    size_t again = im.consume(test_topic, recovered, 10,
                              [](uint32_t, uint64_t, const std::vector<char> &) {});
    std::cout << "Second consumption returned " << again << " messages (expected 0)" << std::endl;

    std::cout << "Test passed." << std::endl;
    return 0;
}
