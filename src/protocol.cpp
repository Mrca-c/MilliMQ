#include "protocol.h"
#include <cstring>
#include <arpa/inet.h>
#include <endian.h>

namespace millimq
{
    namespace protocol
    {

        static uint64_t htonll(uint64_t host)
        {
            return htobe64(host);
        }
        static uint64_t ntohll(uint64_t net)
        {
            return be64toh(net);
        }

        std::vector<char> make_frame(const char *payload, size_t len)
        {
            std::vector<char> frame(4 + len);
            uint32_t net_len = htonl(static_cast<uint32_t>(len));
            memcpy(frame.data(), &net_len, 4);
            memcpy(frame.data() + 4, payload, len);
            return frame;
        }

        bool extract_frame(std::vector<char> &buf, std::vector<char> &out_payload)
        {
            if (buf.size() < 4)
                return false;
            uint32_t len;
            memcpy(&len, buf.data(), 4);
            len = ntohl(len);
            if (buf.size() < 4 + len)
                return false;
            out_payload.assign(buf.begin() + 4, buf.begin() + 4 + len);
            buf.erase(buf.begin(), buf.begin() + 4 + len);
            return true;
        }

        bool parse_produce(const char *data, size_t len, ProduceRequest &req)
        {
            if (len < 1 + 4 + 4)
                return false;
            data += 1;
            len -= 1;
            req.topic_id = ntohl(*reinterpret_cast<const uint32_t *>(data));
            data += 4;
            len -= 4;
            uint32_t payload_len = ntohl(*reinterpret_cast<const uint32_t *>(data));
            data += 4;
            len -= 4;
            if (len < payload_len)
                return false;
            req.payload.assign(data, data + payload_len);
            return true;
        }

        bool parse_consume(const char *data, size_t len, ConsumeRequest &req)
        {
            if (len < 1 + 4 + 8 + 4)
                return false;
            data += 1;
            len -= 1;
            req.topic_id = ntohl(*reinterpret_cast<const uint32_t *>(data));
            data += 4;
            len -= 4;
            req.start_seq = ntohll(*reinterpret_cast<const uint64_t *>(data));
            data += 8;
            len -= 8;
            req.max_count = ntohl(*reinterpret_cast<const uint32_t *>(data));
            return true;
        }

        bool parse_commit(const char *data, size_t len, CommitRequest &req)
        {
            if (len < 1 + 4 + 8)
                return false;
            data += 1;
            len -= 1;
            req.topic_id = ntohl(*reinterpret_cast<const uint32_t *>(data));
            data += 4;
            len -= 4;
            req.next_seq = ntohll(*reinterpret_cast<const uint64_t *>(data));
            return true;
        }

        bool parse_get_offset(const char *data, size_t len, GetOffsetRequest &req)
        {
            if (len < 1 + 4)
                return false;
            data += 1;
            req.topic_id = ntohl(*reinterpret_cast<const uint32_t *>(data));
            return true;
        }

        static void append_uint32(std::vector<char> &v, uint32_t val)
        {
            uint32_t net = htonl(val);
            size_t pos = v.size();
            v.resize(pos + 4);
            memcpy(v.data() + pos, &net, 4);
        }
        static void append_uint64(std::vector<char> &v, uint64_t val)
        {
            uint64_t net = htonll(val);
            size_t pos = v.size();
            v.resize(pos + 8);
            memcpy(v.data() + pos, &net, 8);
        }
        static void append_bytes(std::vector<char> &v, const char *data, size_t len)
        {
            v.insert(v.end(), data, data + len);
        }

        std::vector<char> build_produce_response(uint64_t seq_num)
        {
            std::vector<char> payload;
            payload.push_back((char)Status::Success);
            append_uint64(payload, seq_num);
            return make_frame(payload.data(), payload.size());
        }

        std::vector<char> build_consume_response(const std::vector<std::tuple<uint32_t, uint64_t, std::vector<char>>> &msgs)
        {
            std::vector<char> payload;
            payload.push_back((char)Status::Success);
            append_uint32(payload, static_cast<uint32_t>(msgs.size()));
            for (const auto &[topic, seq, msg] : msgs)
            {
                append_uint32(payload, topic);
                append_uint64(payload, seq);
                append_uint32(payload, static_cast<uint32_t>(msg.size()));
                append_bytes(payload, msg.data(), msg.size());
            }
            return make_frame(payload.data(), payload.size());
        }

        std::vector<char> build_get_offset_response(uint64_t next_seq)
        {
            std::vector<char> payload;
            payload.push_back((char)Status::Success);
            append_uint64(payload, next_seq);
            return make_frame(payload.data(), payload.size());
        }

        std::vector<char> build_commit_response(bool success)
        {
            std::vector<char> payload;
            payload.push_back(success ? (char)Status::Success : (char)Status::Error);
            return make_frame(payload.data(), payload.size());
        }

        std::vector<char> build_error_response()
        {
            std::vector<char> payload;
            payload.push_back((char)Status::Error);
            return make_frame(payload.data(), payload.size());
        }

    }
}