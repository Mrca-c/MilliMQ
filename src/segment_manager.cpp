#include "segment_manager.h"
#include <iostream>
#include <stdexcept>

namespace millimq
{

    SegmentManager::SegmentManager(const std::string &data_dir, uint64_t max_seg_size)
        : dir_(data_dir), max_seg_size_(max_seg_size)
    {
        active_segment_ = std::make_unique<SegmentFile>(next_seg_id_++);
        if (!active_segment_->open_for_write(dir_))
        {
            throw std::runtime_error("无法创建初始文件！");
        }
        current_seg_size_ = 0;
    }

    void SegmentManager::roll_new_segment()
    {
        // 切换下一文件
        active_segment_ = std::make_unique<SegmentFile>(next_seg_id_++);
        if (!active_segment_->open_for_write(dir_))
        {
            throw std::runtime_error("切换文件失败！");
        }
        current_seg_size_ = 0;
        std::cout << "切换到新文件编号 " << active_segment_->segment_id() << std::endl;
    }

    std::pair<uint32_t, uint64_t> SegmentManager::append(const char *data, size_t len)
    {
        // 如果当前文件写不下这条消息（写完后总大小超过上限），就换新
        if (current_seg_size_ + len > max_seg_size_)
        {
            roll_new_segment();
        }

        uint32_t seg_id = active_segment_->segment_id();
        int64_t offset = active_segment_->append(data, len);
        if (offset < 0)
        {
            std::cerr << "写入失败！" << std::endl;
            return {0, 0}; // 返回一个无效值表示错误
        }

        current_seg_size_ += len; // 更新当前文件已写的字节数
        return {seg_id, static_cast<uint64_t>(offset)};
    }

    int64_t SegmentManager::read(uint32_t seg_id, uint64_t offset, char *buf, size_t max_len)
    {
        // 临时翻开指定的旧文件，读完它会在函数结束时自动合上
        SegmentFile seg(seg_id);
        if (!seg.open_for_read(dir_))
        {
            return -1;
        }
        return seg.read_at(offset, buf, max_len);
    }

}