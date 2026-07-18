#include "message_codec.h"
#include <cstring>
#include <stdexcept>

namespace millimq
{

    std::vector<char> MessageCodec::encode(uint32_t topic_id,
                                           const char *payload,
                                           uint32_t payload_len)
    {
        // 预分配足够的内存
        size_t total_len = MessageHeader::SIZE + payload_len;
        std::vector<char> buffer(total_len, 0);
        char *ptr = buffer.data();

        // 1. 写入版本号（1 字节）
        uint8_t version = 0;
        memcpy(ptr, &version, 1);
        ptr += 1;

        // 2. 写入 topic_id（4 字节）
        memcpy(ptr, &topic_id, 4);
        ptr += 4;

        // 3. 写入 payload_len（4 字节）
        memcpy(ptr, &payload_len, 4);
        ptr += 4;

        // 4. 写入真正的消息内容（payload_len 字节）
        memcpy(ptr, payload, payload_len);

        return buffer;
    }

    bool MessageCodec::decode_header(const char *data, MessageHeader &header)
    {
        if (!data)
            return false;

        const char *ptr = data;

        memcpy(&header.version, ptr, 1);
        ptr += 1;

        memcpy(&header.topic_id, ptr, 4);
        ptr += 4;

        memcpy(&header.payload_len, ptr, 4);

        return true;
    }

}