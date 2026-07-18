#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace millimq
{

    // 消息头部(先简易完成，便于测试后续增加内容)
    struct MessageHeader
    {
        uint8_t version;      // 格式版本,用于更新消息头
        uint32_t topic_id;    // 主题的唯一编号
        uint32_t payload_len; // 有效内容长

        static constexpr size_t SIZE = 1 + 4 + 4; // 头部固定 9 字节
    };

    class MessageCodec
    {
    public:
        // 编码
        static std::vector<char> encode(uint32_t topic_id,
                                        const char *payload,
                                        uint32_t payload_len);

        // 解码
        static bool decode_header(const char *data, MessageHeader &header);
    };

}