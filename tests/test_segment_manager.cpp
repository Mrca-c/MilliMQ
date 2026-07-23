#include "../src/segment_manager.h"
#include <cassert>
#include <iostream>
#include <sys/stat.h>
#include <cstring>
#include <vector>
#include <unistd.h>

int main()
{
    const std::string dir = "./test_seg_data";
    mkdir(dir.c_str(), 0755);

    const uint64_t max_seg = 1024; // 1KB 段文件
    const size_t pool_size = 4;

    // ===== 基本写入读取 =====
    {
        millimq::SegmentManager sm(dir, max_seg, pool_size);

        // 写入一条消息
        const char *data = "hello_segment_test";
        size_t len = strlen(data);
        auto [seg_id, offset] = sm.append(data, len);
        assert(seg_id >= 1);
        assert(offset == 0); // 新文件第一个偏移

        // 读取
        char buf[256] = {};
        int64_t n = sm.read(seg_id, offset, buf, sizeof(buf));
        assert(n == (int64_t)len);
        assert(memcmp(buf, data, len) == 0);
    }

    // ===== 段轮转 =====
    {
        millimq::SegmentManager sm(dir, max_seg, pool_size);

        // 写入大于段大小的数据，触发轮转
        std::vector<char> big(800, 'x');
        auto [seg1, off1] = sm.append(big.data(), big.size());
        assert(seg1 >= 1);
        std::cout << "  seg1=" << seg1 << " off1=" << off1 << std::endl;

        // 再写一条，应仍在同一段（800 < 1024）
        std::vector<char> small(300, 'y');
        auto [seg2, off2] = sm.append(small.data(), small.size());
        // 800 + 300 > 1024，应触发轮转
        assert(seg2 > seg1);
        std::cout << "  seg2=" << seg2 << " off2=" << off2 << std::endl;

        // 跨段读取
        char buf1[1024] = {};
        int64_t n1 = sm.read(seg1, off1, buf1, sizeof(buf1));
        assert(n1 == 800);
        assert(buf1[0] == 'x' && buf1[799] == 'x');

        char buf2[1024] = {};
        int64_t n2 = sm.read(seg2, off2, buf2, sizeof(buf2));
        assert(n2 == 300);
        assert(buf2[0] == 'y' && buf2[299] == 'y');
    }

    // 清理
    std::cout << "test_segment_manager: ALL PASSED" << std::endl;

    // 关闭所有句柄后删除测试文件
    // (析构函数自动处理)
    for (uint32_t i = 1; i <= 10; ++i)
    {
        std::string path = dir + "/segment_" + std::to_string(i) + ".log";
        unlink(path.c_str());
    }
    rmdir(dir.c_str());

    return 0;
}
