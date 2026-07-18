#include "segment_file.h"
#include "message_codec.h"
#include <iostream>
#include <sys/stat.h>
#include <vector>
#include <cstring>

int main()
{
    // 确保数据目录存在
    mkdir("./data", 0755);

    // 打开第一个段文件
    millimq::SegmentFile seg(1);
    if (!seg.open_for_write("./data"))
    {
        std::cerr << "Failed to open segment file" << std::endl;
        return 1;
    }

    // 准备一条测试消息
    uint32_t topic_id = 100;
    const char *text = "Hello MilliMQ!";
    uint32_t text_len = strlen(text);

    // 编码
    auto encoded = millimq::MessageCodec::encode(topic_id, text, text_len);
    std::cout << "Encoded size: " << encoded.size() << " bytes" << std::endl;

    // 写入段文件
    int64_t offset = seg.append(encoded.data(), encoded.size());
    if (offset < 0)
    {
        std::cerr << "Append failed" << std::endl;
        return 1;
    }
    std::cout << "Written at offset " << offset << std::endl;

    // 读回数据
    std::vector<char> read_buf(encoded.size());
    int64_t n = seg.read_at(offset, read_buf.data(), read_buf.size());
    if (n < 0)
    {
        std::cerr << "Read failed" << std::endl;
        return 1;
    }

    // 解码头部
    millimq::MessageHeader hdr;
    if (!millimq::MessageCodec::decode_header(read_buf.data(), hdr))
    {
        std::cerr << "Decode header failed" << std::endl;
        return 1;
    }

    // 打印解析结果
    std::cout << "Decoded: version=" << (int)hdr.version
              << " topic=" << hdr.topic_id
              << " payload_len=" << hdr.payload_len << std::endl;

    // 提取有效载荷
    std::string payload(read_buf.data() + millimq::MessageHeader::SIZE, hdr.payload_len);
    std::cout << "Payload: " << payload << std::endl;

    return 0;
}