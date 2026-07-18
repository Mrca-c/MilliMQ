#pragma once
#include "segment_manager.h"
#include <functional>
#include <unordered_map>
#include <vector>

namespace millimq
{
    // 消息结构体
    struct MessagePos
    {
        uint32_t seg_id;    // 在几号段文件
        uint64_t offset;    // 段文件内偏移
        uint32_t total_len; // 消息的总字节数
    };
    // topic索引
    struct TopicIndex
    {
        std::vector<MessagePos> positions;
    };

    class IndexManager
    {
    public:
        IndexManager(SegmentManager &seg_mgr);

        uint64_t produce(uint32_t topic_id, const char *payload, uint32_t payload_len);

        size_t consume(uint32_t topic_id,
                       uint64_t start_seq,
                       size_t max_count,
                       std::function<void(uint32_t topic_id, uint64_t seq_num, const std::vector<char> &payload)> cb);

    private:
        SegmentManager &seg_mgr_;
        std::unordered_map<uint32_t, TopicIndex> topic_map_;  //使用哈希表来存储百万topic的索引表
    };

}