#include "segment_file.h"
#include <iostream>
#include <cstring>
#include <sys/stat.h>

int main()
{
    mkdir("./data", 0755);

    SegmentFile seg(1);
    seg.open_for_write("./data");

    const char *msg = "Hello, MilliMQ!";
    int64_t offset = seg.append(msg, strlen(msg));
    std::cout << "Written at offset " << offset << std::endl;

    char buf[64] = {0};
    int64_t n = seg.read_at(offset, buf, sizeof(buf) - 1);
    std::cout << "Read " << n << " bytes: '" << buf << "'" << std::endl;

    return 0;
}