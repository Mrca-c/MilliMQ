#include "message_codec.h"
#include <cstring>
#include <stdexcept>

namespace millimq
{

    std::vector<char> MessageCodec::encode_v0(uint32_t topic_id,
                                              const char *payload,
                                              uint32_t payload_len)
    {
        // 预分配足够的内存
        size_t total_len = MessageHeader::V0_SIZE + payload_len;
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
    std::vector<char> MessageCodec::encode_v1(uint32_t topic_id,
                                              uint64_t seq_num,
                                              const char *payload,
                                              uint32_t payload_len)
    {
        size_t total_len = MessageHeader::V1_SIZE + payload_len;
        std::vector<char> buffer(total_len, 0);
        char *ptr = buffer.data();
        uint8_t version = 1;
        memcpy(ptr, &version, 1);
        ptr += 1;
        memcpy(ptr, &topic_id, 4);
        ptr += 4;
        memcpy(ptr, &payload_len, 4);
        ptr += 4;
        memcpy(ptr, &seq_num, 8);
        ptr += 8;
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
        //版本确认
        if (header.version == 0)
        {
            memcpy(&header.topic_id, ptr, 4);
            ptr += 4;
            memcpy(&header.payload_len, ptr, 4);
            header.seq_num = 0;
        }
        else if (header.version == 1)
        {
            memcpy(&header.topic_id, ptr, 4);
            ptr += 4;
            memcpy(&header.payload_len, ptr, 4);
            ptr += 4;
            memcpy(&header.seq_num, ptr, 8);
        }
        else
        {
            return false;
        }
        return true;
    }

}