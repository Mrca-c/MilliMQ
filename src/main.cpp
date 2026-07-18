#include "segment_manager.h"
#include "index_manager.h"
#include <iostream>
#include <sys/stat.h>

int main()
{
    mkdir("./data", 0755);
    millimq::SegmentManager sm("./data", 200);
    millimq::IndexManager im(sm);

    im.produce(100, "Hello", 5);
    im.produce(100, "World", 5);
    im.produce(101, "MilliMQ", 7);

    size_t count = im.consume(100, 0, 10,
                              [](uint32_t tid, uint64_t seq, const std::vector<char> &payload)
                              {
                                  std::string msg(payload.begin(), payload.end());
                                  std::cout << "Consumed Topic=" << tid << " Seq=" << seq << " Payload=" << msg << std::endl;
                              });
    std::cout << "Total consumed from Topic 100: " << count << " messages" << std::endl;

    im.consume(101, 0, 10,
               [](uint32_t tid, uint64_t seq, const std::vector<char> &payload)
               {
                   std::string msg(payload.begin(), payload.end());
                   std::cout << "Consumed Topic=" << tid << " Seq=" << seq << " Payload=" << msg << std::endl;
               });

    return 0;
}