#pragma once

#include <string>
#include <cstdint>

// 段文件类
class SegmentFile
{
public:
    explicit SegmentFile(uint32_t segment_id);
    ~SegmentFile();
    // 打开文件准备写入
    bool open_for_write(const std::string &directory);

private:
    u_int32_t seg_id_; // 段编号，用来确定操作文件
    int fd_;           // 文件描述符
};