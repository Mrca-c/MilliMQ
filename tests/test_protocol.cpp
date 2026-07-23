#include "../src/protocol.h"
#include <cassert>
#include <iostream>
#include <cstring>
#include <arpa/inet.h>

static void test_make_extract_frame()
{
    const char *data = "abcdefgh";
    auto frame = millimq::protocol::make_frame(data, 8);

    // 帧 = 4 字节长度 + 8 字节数据
    assert(frame.size() == 12);
    uint32_t net_len;
    memcpy(&net_len, frame.data(), 4);
    assert(ntohl(net_len) == 8);

    // extract_frame
    std::vector<char> buf = frame;
    std::vector<char> out;
    assert(millimq::protocol::extract_frame(buf, out));
    assert(out.size() == 8);
    assert(memcmp(out.data(), data, 8) == 0);
    assert(buf.empty());

    // 数据不足
    std::vector<char> short_buf = {0x00, 0x00, 0x00, 0x10}; // 声称 16 字节
    short_buf.push_back('x');
    assert(!millimq::protocol::extract_frame(short_buf, out));
    assert(short_buf.size() == 5); // 未被消费

    // buf 不足 4 字节
    std::vector<char> tiny = {'a', 'b'};
    assert(!millimq::protocol::extract_frame(tiny, out));
}

static void test_parse_produce()
{
    std::vector<char> p;
    p.push_back(0x01); // cmd
    uint32_t tid = htonl(123);
    p.insert(p.end(), (char *)&tid, (char *)&tid + 4);
    uint32_t plen = htonl(3);
    p.insert(p.end(), (char *)&plen, (char *)&plen + 4);
    p.insert(p.end(), {'a', 'b', 'c'});

    millimq::protocol::ProduceRequest req;
    assert(millimq::protocol::parse_produce(p.data(), p.size(), req));
    assert(req.topic_id == 123);
    assert(req.payload.size() == 3);
    assert(req.payload[0] == 'a');

    // 数据不足
    assert(!millimq::protocol::parse_produce(p.data(), 5, req));
}

static void test_parse_consume()
{
    std::vector<char> p;
    p.push_back(0x02);
    uint32_t tid = htonl(456);
    p.insert(p.end(), (char *)&tid, (char *)&tid + 4);
    uint64_t seq = htobe64(100);
    p.insert(p.end(), (char *)&seq, (char *)&seq + 8);
    uint32_t maxc = htonl(50);
    p.insert(p.end(), (char *)&maxc, (char *)&maxc + 4);

    millimq::protocol::ConsumeRequest req;
    assert(millimq::protocol::parse_consume(p.data(), p.size(), req));
    assert(req.topic_id == 456);
    assert(req.start_seq == 100);
    assert(req.max_count == 50);

    assert(!millimq::protocol::parse_consume(p.data(), 10, req));
}

static void test_parse_commit()
{
    std::vector<char> p;
    p.push_back(0x03);
    uint32_t tid = htonl(789);
    p.insert(p.end(), (char *)&tid, (char *)&tid + 4);
    uint64_t seq = htobe64(999);
    p.insert(p.end(), (char *)&seq, (char *)&seq + 8);

    millimq::protocol::CommitRequest req;
    assert(millimq::protocol::parse_commit(p.data(), p.size(), req));
    assert(req.topic_id == 789);
    assert(req.next_seq == 999);

    assert(!millimq::protocol::parse_commit(p.data(), 5, req));
}

static void test_parse_get_offset()
{
    std::vector<char> p;
    p.push_back(0x04);
    uint32_t tid = htonl(111);
    p.insert(p.end(), (char *)&tid, (char *)&tid + 4);

    millimq::protocol::GetOffsetRequest req;
    assert(millimq::protocol::parse_get_offset(p.data(), p.size(), req));
    assert(req.topic_id == 111);

    assert(!millimq::protocol::parse_get_offset(p.data(), 3, req));
}

static void test_build_responses()
{
    // produce response
    auto r1 = millimq::protocol::build_produce_response(42);
    assert(r1.size() == 4 + 1 + 8); // 4 length + 1 status + 8 seq

    // error response
    auto r2 = millimq::protocol::build_error_response();
    assert(r2.size() == 4 + 1); // 4 length + 1 status

    // commit response
    auto r3 = millimq::protocol::build_commit_response(true);
    assert(r3.size() == 4 + 1);

    // get offset response
    auto r4 = millimq::protocol::build_get_offset_response(100);
    assert(r4.size() == 4 + 1 + 8);

    // consume response (empty)
    std::vector<std::tuple<uint32_t, uint64_t, std::vector<char>>> msgs;
    auto r5 = millimq::protocol::build_consume_response(msgs);
    assert(r5.size() > 4); // 至少包含 status + count
}

int main()
{
    test_make_extract_frame();
    test_parse_produce();
    test_parse_consume();
    test_parse_commit();
    test_parse_get_offset();
    test_build_responses();

    std::cout << "test_protocol: ALL PASSED" << std::endl;
    return 0;
}
