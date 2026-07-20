#include "segment_manager.h"
#include <iostream>
#include <stdexcept>
#include <unistd.h>
#include <cstring>
#include <sys/stat.h>
#include <sys/time.h>

namespace millimq
{
    static uint64_t current_time_ms()
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
    }

    SegmentManager::SegmentManager(const std::string &data_dir, uint64_t max_seg_size, size_t max_open_files)
        : dir_(data_dir), max_seg_size_(max_seg_size), read_pool_(data_dir, max_open_files)
    {
        scan_existing_segments();
        last_cleanup_time_ = current_time_ms();
    }

    void SegmentManager::roll_new_segment()
    {
        // 切换下一文件
        active_segment_ = std::make_unique<SegmentFile>(next_seg_id_++);
        if (!active_segment_->open_for_write(dir_))
        {
            throw std::runtime_error("切换文件失败！");
        }
        current_seg_size_ = 0;
        std::cout << "切换到新文件编号 " << active_segment_->segment_id() << std::endl;
        seg_meta_[active_segment_->segment_id()] = {current_time_ms()};
    }

    std::pair<uint32_t, uint64_t> SegmentManager::append(const char *data, size_t len)
    {
        // 如果当前文件写不下这条消息（写完后总大小超过上限），就换新
        if (current_seg_size_ + len > max_seg_size_)
        {
            roll_new_segment();
        }

        uint32_t seg_id = active_segment_->segment_id();
        int64_t offset = active_segment_->append(data, len);
        if (offset < 0)
        {
            std::cerr << "写入失败！" << std::endl;
            return {0, 0}; // 返回一个无效值表示错误
        }

        current_seg_size_ += len; // 更新当前文件已写的字节数
        uint64_t now = current_time_ms();
        if (now - last_cleanup_time_ > CLEANUP_INTERVAL_MS)
        {
            cleanup_old_segments(24ULL * 3600 * 1000);
            last_cleanup_time_ = now;
        }
        return {seg_id, static_cast<uint64_t>(offset)};
    }

    int64_t SegmentManager::read(uint32_t seg_id, uint64_t offset, char *buf, size_t max_len)
    {
        // 1. 从句柄池获取该段文件的描述符
        int fd = read_pool_.get_fd(seg_id);
        if (fd < 0)
            return -1;
        // 2. 委托 SegmentFile 的静态方法完成实际读取（封装 I/O 细节）
        return SegmentFile::read_from_fd(fd, offset, buf, max_len);
    }

    void SegmentManager::scan_existing_segments()
    {
        uint32_t max_id = 0;
        off_t last_size = 0;

        for (uint32_t id = 1;; ++id)
        {
            std::string path = dir_ + "/segment_" + std::to_string(id) + ".log";
            struct stat st;
            if (stat(path.c_str(), &st) == 0)
            {
                max_id = id;
                last_size = st.st_size;
                seg_meta_[id] = {static_cast<uint64_t>(st.st_mtime) * 1000ULL};
            }
            else
            {
                break;
            }
        }

        if (max_id == 0)
        {
            next_seg_id_ = 1;
            current_seg_size_ = 0;
            active_segment_ = std::make_unique<SegmentFile>(next_seg_id_++);
            if (!active_segment_->open_for_write(dir_))
            {
                throw std::runtime_error("Failed to create initial segment");
            }
            seg_meta_[active_segment_->segment_id()] = {current_time_ms()};
            return;
        }

        next_seg_id_ = max_id + 1;

        if (last_size >= static_cast<off_t>(max_seg_size_))
        {
            roll_new_segment();
        }
        else
        {
            active_segment_ = std::make_unique<SegmentFile>(max_id);
            if (!active_segment_->open_for_write(dir_))
            {
                throw std::runtime_error("Failed to open existing segment for append");
            }
            current_seg_size_ = static_cast<uint64_t>(last_size);
        }
    }

    void SegmentManager::cleanup_old_segments(uint64_t retain_ms)
    {
        uint64_t now = current_time_ms();
        uint64_t expire_time = now - retain_ms; // 计算过期时间点
        auto it = seg_meta_.begin();
        while (it != seg_meta_.end())
        {
            uint32_t seg_id = it->first;
            // 不能删除正在写入的活跃段
            if (active_segment_ && seg_id == active_segment_->segment_id())
            {
                ++it;
                continue;
            }
            if (it->second.create_time < expire_time)
            {
                // 1. 删除磁盘上的段文件
                std::string path = dir_ + "/segment_" + std::to_string(seg_id) + ".log";
                if (unlink(path.c_str()) == 0)
                    std::cout << "Deleted old segment: " << path << std::endl;
                else
                    std::cerr << "Failed to delete segment: " << path << std::endl;
                // 2. 从句柄池中移除
                read_pool_.remove(seg_id);
                // 3. 从映射中移除
                it = seg_meta_.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

}