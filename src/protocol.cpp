#include "protocol.h"
#include <cstring>
#include <arpa/inet.h>

namespace millimq
{
    namespace protocol
    {

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

    }
}