#pragma once
#include <string>
#include <unordered_map>
#include <cstdint>

namespace millimq
{

    // 文件句柄池：限制同时打开的只读段文件的数量
    class FileHandlePool
    {
    public:
        FileHandlePool(const std::string &data_dir, size_t max_open_files);
        ~FileHandlePool();

        int get_fd(uint32_t seg_id);

        void remove(uint32_t seg_id);

        size_t size() const;

    private:
        struct Entry
        {
            int fd;               // 文件描述符
            uint64_t last_access; // 最后访问时间
        };

        uint64_t current_time(); // 返回当前时间计数，内部自增
        void evict_one();        // 淘汰最久未使用的条目

        std::string dir_;                          // 段文件目录
        size_t max_size_;                          // 最大容量
        std::unordered_map<uint32_t, Entry> pool_; // seg_id -> 句柄及访问时间
        uint64_t time_counter_ = 0;                // 简易时间戳计数器
    };

}