#pragma once

#include "segment_file.h"
#include <string>
#include <memory>
#include <cstdint>

namespace millimq
{
    class SegmentManager
    {
    public:
        // 传入文件位置，最大容量
        SegmentManager(const std::string &data_dir, uint64_t max_seg_size);
        ~SegmentManager() = default;
        std::pair<uint32_t, uint64_t> append(const char *data, size_t len);
        int64_t read(uint32_t seg_id, uint64_t offset, char *buf, size_t max_len);

    private:
        void roll_new_segment();                      // 打开新段文件
        std::string dir_;                             // 存放文件的文件夹
        uint64_t max_seg_size_;                       // 最大容量
        uint32_t next_seg_id_ = 1;                    // 下一文件编号
        uint64_t current_seg_size_ = 0;               // 当前文件已写的字节数
        std::unique_ptr<SegmentFile> active_segment_; // 当前文件
    };
}