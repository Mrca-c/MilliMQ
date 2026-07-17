#include "segment_file.h"

int main()
{
    SegmentFile seg(1);
    if (seg.open_for_write("./data"))
    {
    }
    return 0;
}