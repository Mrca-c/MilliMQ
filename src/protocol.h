#pragma once
#include <cstdint>
#include <vector>
#include <string>

#include "index_manager.h"

namespace millimq
{

    enum class Command : uint8_t
    {
        Produce = 0x01,
        Consume = 0x02,
        Commit = 0x03,
        GetOffset = 0x04
    };

    enum class Status : uint8_t
    {
        Success = 0x00,
        Error = 0x01
    };

    namespace protocol
    {

        std::vector<char> make_frame(const char *payload, size_t len);
        bool extract_frame(std::vector<char> &buf, std::vector<char> &out_payload);

        struct ProduceRequest
        {
            uint32_t topic_id;
            std::vector<char> payload;
        };
        struct ConsumeRequest
        {
            uint32_t topic_id;
            uint64_t start_seq;
            uint32_t max_count;
        };
        struct CommitRequest
        {
            uint32_t topic_id;
            uint64_t next_seq;
        };
        struct GetOffsetRequest
        {
            uint32_t topic_id;
        };

        bool parse_produce(const char *data, size_t len, ProduceRequest &req);
        bool parse_consume(const char *data, size_t len, ConsumeRequest &req);
        bool parse_commit(const char *data, size_t len, CommitRequest &req);
        bool parse_get_offset(const char *data, size_t len, GetOffsetRequest &req);

        std::vector<char> build_produce_response(uint64_t seq_num);
        std::vector<char> build_consume_response(const std::vector<std::tuple<uint32_t, uint64_t, std::vector<char>>> &msgs);
        std::vector<char> build_get_offset_response(uint64_t next_seq);
        std::vector<char> build_commit_response(bool success);
        std::vector<char> build_error_response();

    }
}