#include "segment_file.h"
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <cstring>

SegmentFile::SegmentFile(uint32_t segment_id)
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
               O_RDWR | O_CREAT | O_APPEND, // 读写打开，不存在创建，追加写入
               0644);                       // 所有者可读写，group和other只读
    if (fd_ < 0)
    {
        std::cerr << "open Failed" << std::endl;
        return false;
    }
    std::cout << "open success segment_id:" << seg_id_ << std::endl;
    return true;
}

int64_t SegmentFile::append(const char *data, size_t len)
{
    if (fd_ < 0)
        return -1;
    // 先用 lseek 获取当前文件大小，也就是本次写入的起始偏移
    off_t start = lseek(fd_, 0, SEEK_END);
    if (start < 0)
    {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        return -1;
    }
    ssize_t written = write(fd_, data, len);
    if (written < 0)
    {
        std::cerr << "write failed: " << strerror(errno) << std::endl;
        return -1;
    }
    return static_cast<int64_t>(start);
}

int64_t SegmentFile::read_at(uint64_t offset, char *buf, size_t max_len)
{
    if (fd_ < 0)
        return -1;
    // 1. 将文件读写指针移动到想要读取的位置
    off_t ret = lseek(fd_, static_cast<off_t>(offset), SEEK_SET);
    if (ret < 0)
    {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        return -1;
    }
    // 2. 从当前位置读取数据
    ssize_t n = read(fd_, buf, max_len);
    if (n < 0)
    {
        std::cerr << "read failed: " << strerror(errno) << std::endl;
        return -1;
    }
    return static_cast<int64_t>(n);
}