#pragma once

#include <string>
#include <cstdint>

namespace millimq
{
    // 段文件类
    class SegmentFile
    {
    public:
        explicit SegmentFile(uint32_t segment_id);
        ~SegmentFile();
        // 打开文件准备写入
        bool open_for_write(const std::string &directory);
        // 只读方式打开旧段文件
        bool open_for_read(const std::string &directory);
        int64_t append(const char *data, size_t len);                // 返回写入的起始偏移
        int64_t read_at(uint64_t offset, char *buf, size_t max_len); // 返回读取字节数
        // 获得文件id，得知几号段文件
        uint32_t sigment_id() const { return seg_id_; };

    private:
        uint32_t seg_id_; // 段编号，用来确定操作文件
        int fd_ = -1;     // 文件描述符
    };
}
