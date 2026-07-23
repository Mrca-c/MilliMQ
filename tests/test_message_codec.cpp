#include "../src/message_codec.h"
#include <cassert>
#include <iostream>
#include <cstring>

int main()
{
    // ===== v0 编码 =====
    {
        const char *msg = "hello";
        auto buf = millimq::MessageCodec::encode_v0(42, msg, 5);
        assert(buf.size() == millimq::MessageHeader::V0_SIZE + 5);
        assert(buf[0] == 0); // version

        millimq::MessageHeader hdr;
        assert(millimq::MessageCodec::decode_header(buf.data(), hdr));
        assert(hdr.version == 0);
        assert(hdr.topic_id == 42);
        assert(hdr.payload_len == 5);
        assert(hdr.seq_num == 0);
        assert(hdr.timestamp == 0);
    }

    // ===== v1 编码 =====
    {
        const char *msg = "world";
        auto buf = millimq::MessageCodec::encode_v1(7, 12345, msg, 5);
        assert(buf.size() == millimq::MessageHeader::V1_SIZE + 5);

        millimq::MessageHeader hdr;
        assert(millimq::MessageCodec::decode_header(buf.data(), hdr));
        assert(hdr.version == 1);
        assert(hdr.topic_id == 7);
        assert(hdr.payload_len == 5);
        assert(hdr.seq_num == 12345);
        assert(hdr.timestamp == 0);
    }

    // ===== v2 编码 + CRC =====
    {
        const char *msg = "test";
        auto buf = millimq::MessageCodec::encode_v2(1, 0, 1717171717000, msg, 4);
        assert(buf.size() == millimq::MessageHeader::V2_SIZE + 4 + 4); // header + payload + CRC

        millimq::MessageHeader hdr;
        assert(millimq::MessageCodec::decode_header(buf.data(), hdr));
        assert(hdr.version == 2);
        assert(hdr.topic_id == 1);
        assert(hdr.payload_len == 4);
        assert(hdr.seq_num == 0);
        assert(hdr.timestamp == 1717171717000);

        // CRC 校验通过
        assert(millimq::MessageCodec::verify_crc(buf.data(), buf.size()));

        // CRC 校验失败（篡改数据）
        auto bad = buf;
        bad[millimq::MessageHeader::V2_SIZE + 1] ^= 0xFF;
        assert(!millimq::MessageCodec::verify_crc(bad.data(), bad.size()));
    }

    // ===== decode 空指针 =====
    {
        millimq::MessageHeader hdr;
        assert(!millimq::MessageCodec::decode_header(nullptr, hdr));
    }

    // ===== decode 错误版本号 =====
    {
        char bad[1] = {(char)99};
        millimq::MessageHeader hdr;
        assert(!millimq::MessageCodec::decode_header(bad, hdr));
    }

    // ===== 非 v2 数据 CRC 校验 =====
    {
        // v0 数据太短，不足 v2 最小长度，verify_crc 返回 false
        auto v0 = millimq::MessageCodec::encode_v0(1, "ab", 2);
        assert(!millimq::MessageCodec::verify_crc(v0.data(), v0.size()));

        // v1 同样太短
        auto v1 = millimq::MessageCodec::encode_v1(1, 0, "ab", 2);
        assert(!millimq::MessageCodec::verify_crc(v1.data(), v1.size()));
    }

    std::cout << "test_message_codec: ALL PASSED" << std::endl;
    return 0;
}
