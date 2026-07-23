#include "../src/file_handle_pool.h"
#include <cassert>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>

int main()
{
    // 创建临时测试目录和文件
    const std::string dir = "./test_pool_data";
    mkdir(dir.c_str(), 0755);

    // 创建 10 个段文件
    for (uint32_t i = 1; i <= 10; ++i)
    {
        std::string path = dir + "/segment_" + std::to_string(i) + ".log";
        std::ofstream ofs(path);
        ofs << "data_for_segment_" << i;
        ofs.close();
    }

    // ===== 基本打开和获取 =====
    {
        millimq::FileHandlePool pool(dir, 4); // 上限 4 个 fd
        assert(pool.size() == 0);

        // 打开第一个文件
        int fd1 = pool.get_fd(1);
        assert(fd1 >= 0);
        assert(pool.size() == 1);

        // 再次获取同一个 fd（命中缓存）
        int fd1_again = pool.get_fd(1);
        assert(fd1_again == fd1);
        assert(pool.size() == 1);
    }

    // ===== LRU 淘汰 =====
    {
        millimq::FileHandlePool pool(dir, 3);

        // 打开 3 个文件，填满池
        int f1 = pool.get_fd(1);
        int f2 = pool.get_fd(2);
        int f3 = pool.get_fd(3);
        assert(pool.size() == 3);

        // 访问 f1 使其成为最新
        pool.get_fd(1);

        // 打开第 4 个文件，应淘汰最旧的 f2
        int f4 = pool.get_fd(4);
        assert(pool.size() == 3);
        assert(f4 >= 0);

        // f2 应该已被关闭，重新获取会新建 fd
        int f2_new = pool.get_fd(2);
        assert(pool.size() == 3);
        // f2_new 和原 f2 的 fd 号可能不同，但数据应能读到
    }

    // ===== remove =====
    {
        millimq::FileHandlePool pool(dir, 8);
        pool.get_fd(5);
        assert(pool.size() == 1);

        pool.remove(5);
        assert(pool.size() == 0);

        // remove 不存在的 id 不崩溃
        pool.remove(999);
    }

    // ===== 打开不存在的文件 =====
    {
        millimq::FileHandlePool pool(dir, 4);
        int fd = pool.get_fd(99999);
        assert(fd < 0); // 应返回 -1
    }

    // 清理
    for (uint32_t i = 1; i <= 10; ++i)
    {
        std::string path = dir + "/segment_" + std::to_string(i) + ".log";
        unlink(path.c_str());
    }
    rmdir(dir.c_str());

    std::cout << "test_file_handle_pool: ALL PASSED" << std::endl;
    return 0;
}
