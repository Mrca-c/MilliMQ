#include "message_codec.h"
#include <cstring>
#include <stdexcept>

namespace millimq
{
    static uint32_t compute_crc32(const void *data, size_t len)
    {
        static uint32_t table[256];
        static bool table_init = false;
        if (!table_init)
        {
            for (int i = 0; i < 256; ++i)
            {
                uint32_t crc = i;
                for (int j = 0; j < 8; ++j)
                {
                    crc = (crc >> 1) ^ (crc & 1 ? 0xEDB88320 : 0);
                }
                table[i] = crc;
            }
            table_init = true;
        }
        const uint8_t *bytes = static_cast<const uint8_t *>(data);
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < len; ++i)
        {
            crc = (crc >> 8) ^ table[(crc & 0xFF) ^ bytes[i]];
        }
        return crc ^ 0xFFFFFFFF;
    }

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
    std::vector<char> MessageCodec::encode_v2(uint32_t topic_id,
                                              uint64_t seq_num,
                                              uint64_t timestamp,
                                              const char *payload,
                                              uint32_t payload_len)
    {
        size_t total_len = MessageHeader::V2_SIZE + payload_len;
        std::vector<char> buffer(total_len, 0);
        char *ptr = buffer.data();
        uint8_t version = 2;
        memcpy(ptr, &version, 1);
        ptr += 1;
        memcpy(ptr, &topic_id, 4);
        ptr += 4;
        memcpy(ptr, &payload_len, 4);
        ptr += 4;
        memcpy(ptr, &seq_num, 8);
        ptr += 8;
        memcpy(ptr, &timestamp, 8);
        ptr += 8;
        uint32_t crc_placeholder = 0;
        memcpy(ptr, &crc_placeholder, 4);
        ptr += 4;
        memcpy(ptr, payload, payload_len);
        uint32_t crc = compute_crc32(buffer.data(), total_len - 4);
        memcpy(buffer.data() + total_len - 4, &crc, 4);
        return buffer;
    }

    bool MessageCodec::decode_header(const char *data, MessageHeader &header)
    {
        if (!data)
            return false;
        const char *ptr = data;

        memcpy(&header.version, ptr, 1);
        ptr += 1;
        // 版本确认
        if (header.version == 0)
        {
            memcpy(&header.topic_id, ptr, 4);
            ptr += 4;
            memcpy(&header.payload_len, ptr, 4);
            header.seq_num = 0;
            header.timestamp = 0;
            header.crc = 0;
        }
        else if (header.version == 1)
        {
            memcpy(&header.topic_id, ptr, 4);
            ptr += 4;
            memcpy(&header.payload_len, ptr, 4);
            ptr += 4;
            memcpy(&header.seq_num, ptr, 8);
            header.timestamp = 0;
            header.crc = 0;
        }
        else if (header.version == 2)
        {
            memcpy(&header.topic_id, ptr, 4);
            ptr += 4;
            memcpy(&header.payload_len, ptr, 4);
            ptr += 4;
            memcpy(&header.seq_num, ptr, 8);
            ptr += 8;
            memcpy(&header.timestamp, ptr, 8);
            ptr += 8;
            memcpy(&header.crc, ptr, 4);
        }
        else
        {
            return false;
        }
        return true;
    }

    bool MessageCodec::verify_crc(const char *data, size_t total_len)
    {
        if (total_len < MessageHeader::V2_SIZE)
            return false;
        uint8_t version = 0;
        memcpy(&version, data, 1);
        if (version != 2)
            return true;
        uint32_t stored_crc;
        memcpy(&stored_crc, data + total_len - 4, 4);
        uint32_t computed = compute_crc32(data, total_len - 4);
        return stored_crc == computed;
    }

}