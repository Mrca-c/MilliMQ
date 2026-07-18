#include "file_handle_pool.h"
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cstring>

namespace millimq
{

    FileHandlePool::FileHandlePool(const std::string &data_dir, size_t max_open_files)
        : dir_(data_dir), max_size_(max_open_files)
    {
    }

    FileHandlePool::~FileHandlePool()
    {
        // 关闭所有打开的文件
        for (auto &[seg_id, entry] : pool_)
        {
            if (entry.fd >= 0)
            {
                close(entry.fd);
            }
        }
    }

    uint64_t FileHandlePool::current_time()
    {
        return ++time_counter_; // 模拟时间戳
    }

    int FileHandlePool::get_fd(uint32_t seg_id)
    {
        // 1. 如果已经在池中，更新访问时间并返回 fd
        auto it = pool_.find(seg_id);
        if (it != pool_.end())
        {
            it->second.last_access = current_time();
            return it->second.fd;
        }

        // 2. 如果池满，先淘汰一个文件
        if (pool_.size() >= max_size_)
        {
            evict_one();
        }

        // 3. 打开该段文件
        std::string path = dir_ + "/segment_" + std::to_string(seg_id) + ".log";
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0)
        {
            std::cerr << "FileHandlePool: failed to open " << path << " " << strerror(errno) << std::endl;
            return -1;
        }

        // 4. 加入池中，记录访问时间
        uint64_t now = current_time();
        pool_[seg_id] = {fd, now};
        return fd;
    }

    void FileHandlePool::remove(uint32_t seg_id)
    {
        auto it = pool_.find(seg_id);
        if (it != pool_.end())
        {
            if (it->second.fd >= 0)
            {
                close(it->second.fd);
            }
            pool_.erase(it);
        }
    }

    size_t FileHandlePool::size() const
    {
        return pool_.size();
    }

    void FileHandlePool::evict_one()
    {
        if (pool_.empty())
            return;

        // 遍历找到 last_access 最小的条目
        auto oldest = pool_.begin();
        for (auto it = pool_.begin(); it != pool_.end(); ++it)
        {
            if (it->second.last_access < oldest->second.last_access)
            {
                oldest = it;
            }
        }

        // 关闭文件并从池中移除
        if (oldest->second.fd >= 0)
        {
            close(oldest->second.fd);
        }
        pool_.erase(oldest);
    }

}