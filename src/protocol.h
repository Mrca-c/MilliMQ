#pragma once

#include <cstdint>
#include <vector>

// 协议

namespace millimq
{

    enum class Command : uint8_t
    {
        Produce = 0x01,
        Consume = 0x02,
        Commit = 0x03,
        GetOffset = 0x04
    };

    namespace protocol
    {
        std::vector<char> make_frame(const char *payload, size_t len); // 打包

        bool extract_frame(std::vector<char> &buf, std::vector<char> &out_payload); // 拆包
    }

}