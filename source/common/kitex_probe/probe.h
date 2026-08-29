#pragma once

// Kitex 端到端链路打点探针。
//
// 这是为「Kitex × Envoy 端到端时序归因」实验加的侵入式插桩，
// **不适合上游化**，故独立成库，插桩点保持一行式，便于 rebase 时定位与摘除。
//
// 为什么不用 ThriftFilter 而要改源码：
//   thrift_proxy 的 filter 只能观察到 transportBegin/messageBegin/messageEnd/
//   transportEnd 四个事件，看不到 listener accept、连接池 ready、upstream write、
//   响应首字节 —— 而这些正是双跳 sidecar 开销归因最需要的点。
//
// 三个设计约束：
//
//   1. 未采样请求必须近乎零开销。否则在压测下打点本身会主导测量结果
//      （100k QPS × 9 点全量记录 ≈ 每秒百万级事件）。
//   2. 请求路径上不做 IO、不加锁、不分配堆内存。
//      Envoy 是 per-worker 单线程事件循环，thread_local 天然无竞争。
//   3. E1（下游首字节）早于 TTHeader 解析，此刻 trace 未知。
//      必须先记后绑，否则永远拿不到「数据到达 Envoy」的时刻。

#include <cstdint>
#include <string>

#include "envoy/common/time.h"

#include "absl/strings/string_view.h"

namespace Envoy {
namespace KitexProbe {

// 编译期总开关。关闭时所有宏展开为空，用于测量插桩自身的固定成本
// （设计文档 §8.6 的基线组）。
#ifndef KITEX_PROBE_ENABLED
#define KITEX_PROBE_ENABLED 1
#endif

/**
 * 记录一个连接级事件。此时尚不知道属于哪条 trace，也不知道是否采样，
 * 因此无条件记录，待 bindTrace 时决定去留。
 *
 * 只有 E1（下游首字节到达）用这个 —— 它是唯一早于 TTHeader 解析的点。
 */
void connEvent(uint64_t conn_id, absl::string_view point, MonotonicTime mono,
               SystemTime wall);

/**
 * 把某条连接上的待定事件绑定到具体 trace，并确定采样与否。
 *
 * traceparent 取自 TTHeader StrKV 里的 rpc_persist_traceparent。
 * 解析失败或 sampled 位未置时，该连接上的待定事件被丢弃，
 * 后续 rpcEvent 也会快速返回。
 */
void bindTrace(uint64_t conn_id, int32_t seq_id, absl::string_view traceparent,
               MonotonicTime mono, SystemTime wall);

/**
 * 下游读路径的时间戳槽位。
 *
 * 下游读发生时 TTHeader 还没解析，traceparent 还在字节流里，**采样状态在物理上
 * 不可知** —— 所以既不能像 up_* 那样「查一次采样、挂裸标志」，也不该像
 * connEvent 那样往 pending vector 里塞完整 Event（3 个点 × 99% 未采样请求 =
 * 白做的 push_back 与可能的扩容，违反「未采样近乎零开销」这条设计约束）。
 *
 * 改为每连接固定三个 int64 槽位，覆盖式写入：未采样的代价是一次哈希查找加一次
 * store，零分配。bindTrace 时若确认采样，再把槽位转成事件。
 *
 * 与 Kitex 侧 netpoll 探针的「时间戳槽位」是同一个模式。
 */
enum class Slot { DnEpollWake, DnReadvStart, DnReadvDone };
void connSlot(uint64_t conn_id, Slot which, MonotonicTime mono);

/**
 * 记录一个 RPC 级事件。内部先查采样状态，未采样立即返回。
 *
 * 只取单调时钟。wall clock 由 bindTrace 时记下的基准点加上 mono 差值推算 ——
 * wall 只用于粗排序与人眼可读，精确计算一律走 mono（§8.2），
 * 因此没有必要每个点都再读一次 CLOCK_REALTIME。
 * 每点省下一次 vDSO 调用，8 个点约省 175 ns/请求。
 */
void rpcEvent(uint64_t conn_id, absl::string_view point, MonotonicTime mono);

/**
 * Record an RPC event while avoiding a clock read for unbound or unsampled RPCs.
 *
 * This is intended for fine-grained phase probes: checking the binding is cheaper than adding a
 * monotonic clock read to every request merely to discard the event in rpcEvent().
 */
void rpcEventIfSampled(uint64_t conn_id, absl::string_view point, TimeSource& time_source);

/**
 * 记录**响应写出**这类「RPC 已结束但事件还没发生」的点位。
 *
 * 下游 writev 是异步的：conn_manager 里 write() 只入队，真正的 writev 由事件
 * 循环在 rpc_done 之后才执行。普通 rpcEvent 那时已经查不到绑定了。
 *
 * 这里查的是 endRpc 移交过来的 finishing 槽（查不到则回落到 bindings，
 * 覆盖「事件循环先于 deferred delete」的顺序）。last=true 表示这是该 RPC 的
 * 最后一个尾部点位，记完即释放 —— 否则 onWriteReady 后续的空写会继续挂在它名下。
 *
 * **一条连接同时最多容纳一个待写出的 RPC。** 流水线下前一个会被丢弃并计入
 * 统计（`[probe]` 行的「下游写未归属」），因为那种情况下一次 writev 可能
 * 同时写出多个响应，「某个 RPC 的 writev」本就不可拆 —— 宁可丢也不误记。
 */
void rpcEventTail(uint64_t conn_id, absl::string_view point, MonotonicTime mono, bool last);

/**
 * 该连接上当前是否有被采样的 RPC。
 *
 * 只供**非热路径**做一次性门控。通用读路径（ConnectionImpl 的 epoll/readv 点位）
 * 服务全进程所有连接，不能每个事件都查一次 binding；所以在 onPoolReady 里查一次，
 * 把结果作为裸标志挂到上游连接上，热路径只判零。
 */
bool isSampled(uint64_t conn_id);

/**
 * RPC 结束，释放该连接上的绑定状态。
 */
void endRpc(uint64_t conn_id);

/**
 * 配置输出文件与节点名。未调用时探针不落盘（仅在内存中丢弃），
 * 这样单测和未启用打点的部署不会产生副作用。
 */
void configure(const std::string& path, const std::string& node);

/**
 * 探针是否已启用（即是否设了 KITEX_PROBE_PATH）。
 *
 * 供调用方**在开启连接级门控之前**判断，好让未启用时连
 * `kitex_probe_on_` 都保持为假 —— §8.6 的对照组要的是「探针代码在二进制里
 * 但一点都不激活」，读路径上连一次函数调用都不该多做。
 */
bool enabled();

/** 刷盘。进程退出前调用。 */
void flush();

/** 统计：总事件数、已落盘数、因缓冲区满而丢弃的数量。 */
struct Stats {
  uint64_t recorded;
  uint64_t written;
  uint64_t dropped;
};
Stats stats();

} // namespace KitexProbe
} // namespace Envoy

#if KITEX_PROBE_ENABLED

#define KITEX_PROBE_CONN(conn_id, point, time_source)                                              \
  ::Envoy::KitexProbe::connEvent((conn_id), (point), (time_source).monotonicTime(),                \
                                 (time_source).systemTime())

#define KITEX_PROBE_BIND(conn_id, seq_id, traceparent, time_source)                                \
  ::Envoy::KitexProbe::bindTrace((conn_id), (seq_id), (traceparent),                               \
                                 (time_source).monotonicTime(), (time_source).systemTime())

#define KITEX_PROBE(conn_id, point, time_source)                                                   \
  ::Envoy::KitexProbe::rpcEvent((conn_id), (point), (time_source).monotonicTime())

#define KITEX_PROBE_IF_SAMPLED(conn_id, point, time_source)                                        \
  ::Envoy::KitexProbe::rpcEventIfSampled((conn_id), (point), (time_source))

#define KITEX_PROBE_END(conn_id) ::Envoy::KitexProbe::endRpc((conn_id))

#define KITEX_PROBE_SAMPLED(conn_id) ::Envoy::KitexProbe::isSampled((conn_id))

// 记录 RPC 尾部（响应已入队、writev 稍后执行）的点位。
#define KITEX_PROBE_TAIL(conn_id, point, time_source, last)                                        \
  ::Envoy::KitexProbe::rpcEventTail((conn_id), (point), (time_source).monotonicTime(), (last))

// 记录一个**已经采好**的时刻，而不是「现在」。
// 用于时刻来自别处的场景，例如 epoll 返回的时间由 libevent 的 check 回调
// 提前记下（Envoy 的 approximateMonotonicTime），到 onFileEvent 里再补记。
#define KITEX_PROBE_AT(conn_id, point, mono)                                                       \
  ::Envoy::KitexProbe::rpcEvent((conn_id), (point), (mono))

// 写下游读路径的时间戳槽位。与 KITEX_PROBE_AT 一样接受「已经采好的时刻」。
#define KITEX_PROBE_SLOT(conn_id, slot, mono)                                                      \
  ::Envoy::KitexProbe::connSlot((conn_id), ::Envoy::KitexProbe::Slot::slot, (mono))

#else

#define KITEX_PROBE_CONN(conn_id, point, time_source)                                              \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE_BIND(conn_id, seq_id, traceparent, time_source)                                \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE(conn_id, point, time_source)                                                   \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE_IF_SAMPLED(conn_id, point, time_source)                                        \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE_END(conn_id)                                                                   \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE_SAMPLED(conn_id) false
#define KITEX_PROBE_AT(conn_id, point, mono)                                                       \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE_SLOT(conn_id, slot, mono)                                                      \
  do {                                                                                             \
  } while (0)
#define KITEX_PROBE_TAIL(conn_id, point, time_source, last)                                        \
  do {                                                                                             \
  } while (0)

#endif
