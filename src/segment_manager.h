#pragma once

#include "segment_file.h"
#include "file_handle_pool.h"
#include <string>
#include <memory>
#include <cstdint>
#include <map>

namespace millimq
{
    struct SegmentMeta
    {
        uint64_t create_time; // 段的创建时间
    };

    class SegmentManager
    {
    public:
        // 传入文件位置，最大容量
        SegmentManager(const std::string &data_dir, uint64_t max_seg_size, size_t max_open_files = 8);
        ~SegmentManager() = default;
        std::pair<uint32_t, uint64_t> append(const char *data, size_t len);
        int64_t read(uint32_t seg_id, uint64_t offset, char *buf, size_t max_len);
        // 删除过期文件
        void cleanup_old_segments(uint64_t retain_ms);
        const std::string &get_directory() const { return dir_; }
        size_t debug_pool_size() const { return read_pool_.size(); } // 测试用查看打开文件数

    private:
        void roll_new_segment();                      // 打开新段文件
        void scan_existing_segments();                // 启动时检测文件状态，避免文件无限增长
        std::string dir_;                             // 存放文件的文件夹
        uint64_t max_seg_size_;                       // 最大容量
        uint32_t next_seg_id_ = 1;                    // 下一文件编号
        uint64_t current_seg_size_ = 0;               // 当前文件已写的字节数
        std::unique_ptr<SegmentFile> active_segment_; // 当前文件
        FileHandlePool read_pool_;                    // 句柄池
        std::map<uint32_t, SegmentMeta> seg_meta_;    // 记录段时间信息
    };
}