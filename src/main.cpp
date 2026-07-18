#include "segment_manager.h"
#include "message_codec.h"
#include <iostream>
#include <vector>
#include <sys/stat.h>
#include <cstring>

int main()
{
    mkdir("./data", 0755);

    millimq::SegmentManager sm("./data", 30);

    auto write_msg = [&](const char *text, uint32_t topic_id)
    {
        uint32_t len = strlen(text);
        auto encoded = millimq::MessageCodec::encode(topic_id, text, len);
        auto [seg_id, offset] = sm.append(encoded.data(), encoded.size());
        std::cout << "写入消息到 segment " << seg_id << " 偏移 " << offset << std::endl;
        return std::make_pair(seg_id, offset);
    };

    auto pos1 = write_msg("Hello", 100);   // 这条消息编码后大约 14 字节
    auto pos2 = write_msg("World", 101);   // 再写 14 字节，总共 28，空间还够
    auto pos3 = write_msg("MilliMQ", 100); // 再写大概 16 字节，会超过 30，触发换新本

    // 读取并打印第一条和第三条消息验证
    auto read_msg = [&](uint32_t seg_id, uint64_t offset)
    {
        std::vector<char> buf(64);
        int64_t n = sm.read(seg_id, offset, buf.data(), buf.size());
        if (n > 0)
        {
            millimq::MessageHeader hdr;
            millimq::MessageCodec::decode_header(buf.data(), hdr);
            std::string payload(buf.data() + millimq::MessageHeader::SIZE, hdr.payload_len);
            std::cout << "读出消息: topic=" << hdr.topic_id << " 内容=" << payload << std::endl;
        }
        else
        {
            std::cerr << "读取失败" << std::endl;
        }
    };

    std::cout << "--- 读取验证 ---" << std::endl;
    read_msg(pos1.first, pos1.second);
    read_msg(pos3.first, pos3.second);

    return 0;
}