#include "segment_file.h"
#include <unistd.h>
#include <fcntl.h>
#include <iostream>

SegmentFile::SegmentFile(u_int32_t segment_id)
    : seg_id_(segment_id)
{
}

SegmentFile::~SegmentFile()
{
    if (fd_ != -1)
    {
        close(fd_);
    }
}

bool SegmentFile::open_for_write(const std::string &directory)
{
    std::string path = directory + "/segment_" + std::to_string(seg_id_) + ".log"; // 拼出文件路径
    fd_ = open(path.c_str(),
               O_WRONLY | O_CREAT | O_APPEND, // 只写，不存在创建，追加写入
               0644);                         // 所有者可读写，group和other只读
    if (fd_ < 0)
    {
        std::cerr << "open Failed" << std::endl;
        return false;
    }
    std::cout << "open success segment_id:" << seg_id_ << std::endl;
    return true;
}