#include "index_manager.h"
#include "message_codec.h"
#include <iostream>
#include <cstring>

namespace millimq
{

    IndexManager::IndexManager(SegmentManager &seg_mgr) : seg_mgr_(seg_mgr) {}
    //生产消息，返回消息序号
    uint64_t IndexManager::produce(uint32_t topic_id, const char *payload, uint32_t payload_len)
    {
        //获取或创建topic索引
        auto &topic_index = topic_map_[topic_id];
        //获取已有序号
        uint64_t seq_num = topic_index.positions.size();

        auto encoded = MessageCodec::encode_v1(topic_id, seq_num, payload, payload_len);
        uint32_t total_len = static_cast<uint32_t>(encoded.size());
        //委托写入段文件
        auto [seg_id, offset] = seg_mgr_.append(encoded.data(), total_len);
        if (seg_id == 0 && offset == 0)
        {
            std::cerr << "produce failed: append error" << std::endl;
            return UINT64_MAX;
        }
        //给positions添加最新MessagePos
        topic_index.positions.push_back({seg_id, static_cast<uint64_t>(offset), total_len});
        return seq_num;
    }

    size_t IndexManager::consume(uint32_t topic_id,
                                 uint64_t start_seq,
                                 size_t max_count,
                                 std::function<void(uint32_t, uint64_t, const std::vector<char> &)> cb)
    {
        //查找索引
        auto it = topic_map_.find(topic_id);
        if (it == topic_map_.end() || start_seq >= it->second.positions.size())
            return 0;
        //找到topic对应的positions
        const auto &positions = it->second.positions;
        size_t count = 0;
        for (uint64_t i = start_seq; i < positions.size() && count < max_count; ++i)
        {
            const auto &pos = positions[i];
            std::vector<char> buf(pos.total_len);
            int64_t n = seg_mgr_.read(pos.seg_id, pos.offset, buf.data(), pos.total_len);
            if (n < 0 || static_cast<uint32_t>(n) != pos.total_len)
            {
                std::cerr << "consume read error at seg " << pos.seg_id << " offset " << pos.offset << std::endl;
                break;
            }
            MessageHeader hdr;
            if (!MessageCodec::decode_header(buf.data(), hdr))
            {
                std::cerr << "consume decode header error" << std::endl;
                break;
            }
            if (hdr.topic_id != topic_id || hdr.seq_num != i)
            {
                std::cerr << "consume metadata mismatch" << std::endl;
                break;
            }
            size_t header_size = (hdr.version == 0) ? MessageHeader::V0_SIZE : MessageHeader::V1_SIZE;
            std::vector<char> payload(buf.begin() + header_size, buf.begin() + header_size + hdr.payload_len);
            cb(topic_id, i, payload);
            ++count;
        }
        return count;
    }

}