# MilliMQ

面向百万级小 Topic 场景的消息存储引擎，C++17 实现。

## 背景

Kafka 在大量小 Topic 场景下存在两个问题：一是 Topic 数量与文件句柄数成正比，十万 Topic 意味着十万个 fd；二是每个 Topic 独立写文件带来随机 I/O，影响写入吞吐。

MilliMQ 将所有 Topic 的消息混合写入少量顺序文件，通过 LRU 句柄池将进程同时打开的文件数恒定控制在 8 个，同时消除随机 I/O。

## 构建

```bash
mkdir build && cd build && cmake .. && make
```

生成四个可执行文件：

| 目标 | 说明 |
|------|------|
| `millimq_server` | TCP 服务端，监听 9999 端口 |
| `stress_test [数量]` | 本地直连压测，默认 10 万 Topic |
| `test_client` | 功能测试：生产、消费、偏移读写 |
| `bench_produce [数量] [批次]` | TCP 网络压测，默认 5 万条，支持分批 |

## 架构

```
客户端 ──TCP──> epoll 单线程服务端 ──> 协议解析
                                       │
                                 IndexManager
                                 内存 topic 索引
                                 内存 consumer 偏移
                                       │
                                 SegmentManager
                                 混合日志 + 段文件轮转
                                       │
                                 FileHandlePool
                                 LRU 淘汰，上限 8 个 fd
                                       │
                                 segment_{id}.log
                                 顺序追加文件
```

### 段文件

消息以追加方式写入 `segment_{id}.log`，单文件达到 1MB 后自动创建新文件写入。启动时扫描已有段文件恢复写入位置。过期段文件（默认 24 小时）由 `cleanup_old_segments` 定期清理。

### 句柄池

`FileHandlePool` 以 LRU 策略管理只读段文件的 fd。消费消息时通过句柄池获取 fd，池满时淘汰最久未使用的条目。进程同时打开的文件数不受段文件总数影响，始终在 `max_open_files`（默认 8）以内。

### 消息格式

支持三个版本，向前兼容：

| 版本 | 头部 | 字段 |
|------|------|------|
| v0 | 9 字节 | version + topic_id + payload_len |
| v1 | 17 字节 | v0 + seq_num |
| v2 | 25 字节 + 4 字节 CRC | v1 + timestamp + CRC32 |

默认使用 v2，尾部 4 字节为 CRC32 校验值，覆盖头部和载荷。

### 索引

`IndexManager` 使用 `unordered_map<topic_id, vector<MessagePos>>` 维护每个 Topic 的消息物理位置（段文件编号、段内偏移、总长度）。服务启动时扫描全部段文件重建索引，此后生产时追加写入，消费时按索引定位读取。

### 通信协议

帧格式为 4 字节长度前缀（网络序）+ 命令载荷。支持四种命令：

| 命令 | 值 | 载荷格式 |
|------|-----|---------|
| Produce | 0x01 | topic_id(4B) + payload_len(4B) + payload |
| Consume | 0x02 | topic_id(4B) + start_seq(8B) + max_count(4B) |
| Commit | 0x03 | topic_id(4B) + next_seq(8B) |
| GetOffset | 0x04 | topic_id(4B) |

服务端基于 epoll（level-trigger）单线程处理，非阻塞 I/O。连接建立时读取首批数据并处理，之后通过 epoll 事件循环持续读写。

## 压测数据

环境：普通云主机，单线程，每条消息同步 lseek + write 落盘，无批量提交。

| 场景 | 规模 | 耗时 | 吞吐 | 句柄数 |
|------|------|------|------|--------|
| 本地生产 | 10 万 Topic | ~0.4s | 25 万 msg/s | 8 |
| 本地生产 | 100 万 Topic | ~2.9s | 35 万 msg/s | 8 |
| TCP 生产 | 5 万条 | ~2.2s | 2.3 万 msg/s | 8 |
| TCP 生产 | 100 万条 | ~23s | 4.3 万 msg/s | 8 |

## 已知限制

- 消费偏移存于内存，重启丢失，消费者组仅支持单一 `default` 组
- 索引全内存维护，Topic 数和消息量受机器内存限制
- 单线程服务端，高并发下存在瓶颈
