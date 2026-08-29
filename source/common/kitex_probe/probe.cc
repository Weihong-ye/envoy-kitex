#include "source/common/kitex_probe/probe.h"

#include <atomic>
#include <memory>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sys/syscall.h>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"

namespace Envoy {
namespace KitexProbe {
namespace {

// W3C traceparent: 00-<32 hex trace-id>-<16 hex span-id>-<2 hex flags>
// flags 的 bit0 是 sampled。
constexpr size_t TraceparentLen = 55;
constexpr size_t TraceIdOffset = 3;
constexpr size_t TraceIdLen = 32;
constexpr size_t FlagsOffset = 53;

struct Parsed {
  bool valid{false};
  bool sampled{false};
  absl::string_view trace_id;
};

Parsed parseTraceparent(absl::string_view tp) {
  Parsed p;
  if (tp.size() != TraceparentLen || tp[2] != '-' || tp[35] != '-' || tp[52] != '-') {
    return p;
  }
  const char hi = tp[FlagsOffset];
  const char lo = tp[FlagsOffset + 1];
  auto hex = [](char c) -> int {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    return -1;
  };
  const int h = hex(hi), l = hex(lo);
  if (h < 0 || l < 0) {
    return p;
  }
  p.valid = true;
  p.sampled = ((h << 4) | l) & 0x01;
  p.trace_id = tp.substr(TraceIdOffset, TraceIdLen);
  return p;
}

// Event 不再自带 trace 字符串。
//
// trace-id 是 32 字符，超过 SSO 阈值必然堆分配；而它在一次 RPC 内不变，
// 每个点都拷一份等于每请求多做 8 次 malloc。改成共享同一份：
// binding 持有 shared_ptr<const std::string>，事件只拷指针。
// point 是短名字（SSO 内，无分配），且都是字面量，直接存 string_view。
struct Event {
  std::shared_ptr<const std::string> trace;
  absl::string_view point; // 指向静态字面量，生命周期长于事件
  int64_t mono_ns;
  int64_t wall_ns;
  int32_t seq_id;
};

// 某条连接上「已解析出 trace、正在处理」的 RPC 状态。
struct Binding {
  std::shared_ptr<const std::string> trace;
  int32_t seq_id{0};
  bool sampled{false};
  // wall/mono 的对应基准点，用于由 mono 推算 wall，省去每点一次 CLOCK_REALTIME
  int64_t base_mono{0};
  int64_t base_wall{0};
};

// 下游读路径的三个时间戳，每连接一份，覆盖式写入。
//
// 用固定槽位而不是 pending vector：见 probe.h 里 connSlot 的注释。
// 0 表示「本次没记到」—— 例如响应与请求落在同一次 readv 里、
// 或连接复用后压根没有新的 epoll 唤醒。取的时候按 0 跳过，**不补零**：
// 补零会把「没发生」画成「耗时 0」。
struct ConnSlots {
  int64_t epoll_wake{0};
  int64_t readv_start{0};
  int64_t readv_done{0};
};

// 每 worker 线程一份。Envoy 的 worker 是单线程事件循环，
// 所以这里既不需要锁，也不存在伪共享。
class ThreadState {
public:
  // 线程退出时把本线程剩余的事件落盘。**这是收尾的唯一正确位置**：
  // 稳态路径完全不必为此付出代价，而且刷的是自己的缓冲区，
  // 与「主线程在 atexit 里去刷别人的 vector」不同，不存在数据竞争。
  //
  // 必须挂在 ThreadState 上，不能像以前那样单独搞一个命名空间作用域的
  // `thread_local ThreadFlusher`：那种写法**从来没生效过** ——
  // 命名空间作用域的 thread_local 只在被 odr-use 时才初始化，
  // 而那个对象全代码没有任何地方引用它，于是永远不构造、
  // __cxa_thread_atexit 永远不注册、析构永远不跑。
  // 表现为流量停止后尾部事件静默丢失（实测 1000 请求丢最后 10 条，
  // 落盘量恰好卡在 FlushThreshold 的整数倍）。
  // tls() 里的 `static thread_local ThreadState` 是**函数局部**的，
  // 首次调用必定构造，因此挂在这里的析构有保证。
  ~ThreadState();

  // E1 用：此刻还不知道 trace，先按连接暂存。
  // 一条连接上同时最多有少量待定事件（通常 1 个），用小 vector 足够。
  absl::flat_hash_map<uint64_t, std::vector<Event>> pending;
  absl::flat_hash_map<uint64_t, Binding> bindings;
  // 「响应已入队、writev 还没执行」的那个 RPC。
  //
  // 下游 writev 是异步的，执行时 RPC 早已 rpc_done。曾经为此干脆不擦 bindings，
  // 但那在流水线下会出错：请求 N 的 writev 还没跑，N+1 的 bindTrace 就把
  // bindings[conn] 覆盖了，N 的写会被记到 N+1 头上。
  //
  // 改为单独一个槽：endRpc 把 binding 移到这里，bindings 照常擦（isSampled
  // 等语义不变）。下游 writev 只查这里，写完即清。
  //
  // **一条连接同时最多容纳一个「待写出」的 RPC。** 若前一个还没写出就又来一个，
  // 计入 g_write_lost 并丢弃 —— 宁可丢也不能误记。这也是诚实的：流水线下
  // 一次 writev 可能同时写出多个响应，「某个 RPC 的 writev」本就不可拆。
  absl::flat_hash_map<uint64_t, Binding> finishing;
  // 下游读的时间戳槽位，与 pending 一样按下游 conn_id 索引，
  // 同样在 endRpc 里清理，避免随连接数无界增长。
  absl::flat_hash_map<uint64_t, ConnSlots> slots;
  std::vector<Event> ready;
  int64_t last_flush_ns{0};
  // 每个 worker 线程写自己的文件。
  //
  // 曾经所有线程都 fopen(path,"a") 追加写同一个文件，结果 JSON 行被切碎重组
  // ——O_APPEND 只保证单次 write 的原子性，而批量刷盘会跨多次 write，
  // 于是不同线程的输出互相穿插。表现为 merge 出来的节点名是拼接乱码。
  // 每线程独立文件既解决了交错，也避免了在刷盘路径上加锁。
  std::string path;
  // 常开的文件句柄。每次 writeOut 都 fopen/fclose 的话，
  // 在高 QPS 下 open 系统调用本身就会成为可观的开销。
  FILE* fp{nullptr};
};

ThreadState& tls() {
  static thread_local ThreadState state;
  return state;
}

// 输出配置是进程级的，首次使用时从环境变量读取，之后只读。
//
// 用环境变量而不是扩展 bootstrap schema：探针是实验性的侵入式插桩，
// 不该污染 Envoy 的配置接口；而且这样同一个二进制既能带探针跑，
// 也能不设变量当普通 Envoy 用（§8.6 的对照组）。
//
//   KITEX_PROBE_PATH=/tmp/kitex-demo/trace-envoy-out.ndjson
//   KITEX_PROBE_NODE=envoy-out
struct Config {
  std::string path;
  std::string node;
  std::string host;
  bool enabled{false};
};

// 进程退出时把探针自身的统计写进 stderr（即 Envoy 的日志，
// run-*.sh 会 `tee` 进 envoy-*.log）。定义在下面，这里只前置声明 ——
// config() 在初始化时注册它。
void reportStatsAtExit();

Config& config() {
  static Config c = [] {
    Config init;
    const char* path = std::getenv("KITEX_PROBE_PATH");
    const char* node = std::getenv("KITEX_PROBE_NODE");
    if (path != nullptr && *path != '\0') {
      init.path = path;
      init.node = (node != nullptr && *node != '\0') ? node : "envoy";
      // host 标识决定 merge 工具是否允许把两个事件的时间戳相减（§8.2），
      // 因此必须能真正区分机器。
      //
      // **不能只靠 gethostname**：本实验的两台机器 hostname 都是
      // localhost.localdomain，靠它判断会把跨机误判成同机，
      // 于是跨机相减这一最危险的操作反而畅通无阻。
      // 所以优先取显式配置的 KITEX_PROBE_HOST。
      const char* host = std::getenv("KITEX_PROBE_HOST");
      if (host != nullptr && *host != '\0') {
        init.host = host;
      } else {
        char hostname[256] = {0};
        init.host = (::gethostname(hostname, sizeof(hostname) - 1) == 0) ? hostname : "unknown";
      }
      init.enabled = true;
      // 只在真正启用探针时才注册，未设 KITEX_PROBE_PATH 的普通 Envoy
      // 不受任何影响（§8.6 的对照组）。
      //
      // atexit 而不是 Envoy 的 ServerLifecycleNotifier：探针刻意与 server
      // 完全解耦（配置走环境变量、不进 bootstrap schema），挂生命周期钩子
      // 就得让 source/server 反向依赖这个实验性库，rebase 时更难摘除。
      // 收尾时机上两者等价 —— Envoy 收到 SIGTERM 后是正常返回 main 退出的，
      // worker 线程在此之前已 join，各自的 ThreadState 析构已经刷过盘了。
      std::atexit(&reportStatsAtExit);
    }
    return init;
  }();
  return c;
}

std::atomic<uint64_t> g_recorded{0};
std::atomic<uint64_t> g_written{0};
std::atomic<uint64_t> g_dropped{0};
// 下游 writev 无法归属的次数（前一个响应还没写出，新请求就已绑定）。
// 不为零说明连接上出现了流水线，此时下游写侧的分解不完整。
std::atomic<uint64_t> g_write_lost{0};

// ready 累积到这个数量就刷盘。
// 取值权衡：太小则频繁 IO 影响请求路径，太大则进程异常退出时丢失过多。
constexpr size_t FlushThreshold = 512;
// 单线程最多缓存多少事件。超过即丢弃并计数 —— 宁可丢数据也不能阻塞请求，
// 那会直接改变被测系统的行为。
constexpr size_t MaxBuffered = 1 << 16;

int64_t monoNs(MonotonicTime t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}
int64_t wallNs(SystemTime t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

void writeOut(ThreadState& st, std::vector<Event>& events) {
  if (events.empty()) {
    return;
  }
  auto& cfg = config();
  if (!cfg.enabled) {
    events.clear();
    return;
  }
  if (st.fp == nullptr) {
    st.path = absl::StrFormat("%s.%d", cfg.path, static_cast<int>(::syscall(SYS_gettid)));
    st.fp = std::fopen(st.path.c_str(), "a");
  }
  FILE* f = st.fp;
  if (f == nullptr) {
    events.clear();
    return;
  }
  for (const auto& e : events) {
    // 字段与 Kitex 侧 probe 包保持一致，merge 工具才能统一处理。
    // wall 仅供粗排序，跨机相减由 merge 工具拒绝（设计文档 §8.2）。
    absl::FPrintF(f,
                  R"({"host":"%s","node":"%s","trace":"%s","point":"%s","wall_ns":%d,"mono_ns":%d,)"
                  R"("attrs":{"seq_id":"%d"}})"
                  "\n",
                  cfg.host, cfg.node, *e.trace, e.point, e.wall_ns, e.mono_ns, e.seq_id);
  }
  g_written.fetch_add(events.size(), std::memory_order_relaxed);
  events.clear();
}

ThreadState::~ThreadState() {
  writeOut(*this, ready);
  if (fp != nullptr) {
    std::fflush(fp);
    std::fclose(fp);
    fp = nullptr;
  }
}

void reportStatsAtExit() {
  const Config& cfg = config();
  if (!cfg.enabled) {
    return;
  }
  const uint64_t recorded = g_recorded.load(std::memory_order_relaxed);
  const uint64_t written = g_written.load(std::memory_order_relaxed);
  const uint64_t dropped = g_dropped.load(std::memory_order_relaxed);
  // 格式与 Kitex 侧 demo/probe 的收尾行保持一致，
  // `grep "\[probe\]" *.log` 一次覆盖四个节点。
  //
  // 完整性判据是「记录==落盘 且 丢弃=0」，不是数行数 ——
  // 采样率与并发都会让期望行数没法事先算出来。
  const uint64_t wlost = g_write_lost.load(std::memory_order_relaxed);
  std::fprintf(stderr,
               "[probe] node=%s host=%s 记录=%llu 落盘=%llu 丢弃=%llu 下游写未归属=%llu%s\n",
               cfg.node.c_str(), cfg.host.c_str(), static_cast<unsigned long long>(recorded),
               static_cast<unsigned long long>(written),
               static_cast<unsigned long long>(dropped), static_cast<unsigned long long>(wlost),
               (recorded == written && dropped == 0) ? "" : "  ← 有数据未落盘");
  if (wlost > 0) {
    std::fprintf(stderr,
                 "[probe] 提示：下游写有 %llu 次无法归属（同一连接上出现流水线）。"
                 "下游 writev 那两段不完整，其余点位不受影响。\n",
                 static_cast<unsigned long long>(wlost));
  }
  std::fflush(stderr);
}

// 距上次刷盘超过这个时间就刷一次，即使没攒够 FlushThreshold。
// 没有它的话，低速率场景（功能验证、低采样率压测）会一直攒在内存里不落盘，
// 表现为「明明跑通了却没有 trace 文件」。
constexpr int64_t FlushIntervalNs = 200 * 1000 * 1000; // 200ms

void push(ThreadState& st, Event&& e) {
  if (st.ready.size() >= MaxBuffered) {
    g_dropped.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  const int64_t now = e.mono_ns;
  st.ready.push_back(std::move(e));
  g_recorded.fetch_add(1, std::memory_order_relaxed);

  // 用事件自带的时间戳判断，避免为了刷盘再读一次时钟。
  if (st.last_flush_ns == 0) {
    st.last_flush_ns = now;
  }
  if (st.ready.size() >= FlushThreshold || (now - st.last_flush_ns) >= FlushIntervalNs) {
    writeOut(st, st.ready);
    st.last_flush_ns = now;
  }
}

} // namespace

void configure(const std::string& path, const std::string& node) {
  auto& c = config();
  c.path = path;
  c.node = node;
  c.enabled = !path.empty();
}

bool enabled() { return config().enabled; }

void connEvent(uint64_t conn_id, absl::string_view point, MonotonicTime mono, SystemTime wall) {
  if (!config().enabled) {
    return;
  }
  auto& st = tls();
  auto& vec = st.pending[conn_id];
  // 单条连接上待定事件不应堆积。若堆积说明 bindTrace 没被调用
  // （例如解码失败的连接），直接丢弃最早的，避免无界增长。
  if (vec.size() > 8) {
    vec.erase(vec.begin());
  }
  vec.push_back(Event{nullptr, point, monoNs(mono), wallNs(wall), 0});
}

void connSlot(uint64_t conn_id, Slot which, MonotonicTime mono) {
  if (!config().enabled) {
    return;
  }
  // 这就是整个热路径：一次哈希查找 + 一次 store。不分配、不判采样
  // （此刻也判不了），未采样请求付的就是这点代价。
  ConnSlots& s = tls().slots[conn_id];
  const int64_t m = monoNs(mono);
  switch (which) {
  case Slot::DnEpollWake:
    s.epoll_wake = m;
    break;
  case Slot::DnReadvStart:
    s.readv_start = m;
    break;
  case Slot::DnReadvDone:
    s.readv_done = m;
    break;
  }
}

void bindTrace(uint64_t conn_id, int32_t seq_id, absl::string_view traceparent, MonotonicTime mono,
               SystemTime wall) {
  if (!config().enabled) {
    return;
  }
  auto& st = tls();
  const Parsed p = parseTraceparent(traceparent);

  Binding b;
  b.seq_id = seq_id;
  b.base_mono = monoNs(mono);
  b.base_wall = wallNs(wall);
  if (p.valid) {
    // 整条 RPC 只在这里构造一次 trace 字符串
    b.trace = std::make_shared<const std::string>(p.trace_id);
    b.sampled = p.sampled;
  }
  st.bindings[conn_id] = b;

  // 采样确认了，才把下游读的三个槽位兑现成事件。
  //
  // wall 由基准点推算（与 rpcEvent 同法），不回头读 CLOCK_REALTIME ——
  // 读路径上本来就只采了 mono，这里也不该为了补 wall 再多一次 vDSO 调用。
  //
  // 槽位为 0 表示这一次没记到（响应与请求落在同一次 readv、或连接复用后
  // 没有新的 epoll 唤醒），跳过即可，**不补零**。
  if (b.sampled) {
    auto slot_it = st.slots.find(conn_id);
    if (slot_it != st.slots.end()) {
      const ConnSlots& s = slot_it->second;
      const auto emit = [&](int64_t m, absl::string_view point) {
        if (m != 0) {
          push(st, Event{b.trace, point, m, b.base_wall + (m - b.base_mono), seq_id});
        }
      };
      emit(s.epoll_wake, absl::string_view("dn_epoll_wake"));
      emit(s.readv_start, absl::string_view("dn_readv_start"));
      emit(s.readv_done, absl::string_view("dn_readv_done"));
    }
  }

  auto it = st.pending.find(conn_id);
  if (it == st.pending.end()) {
    return;
  }
  if (b.sampled) {
    // 回填：把 E1 那批事件挂到刚解析出的 trace 上。
    // 不做这一步，「下游首字节到达」这个点就永远无法与其余点关联。
    for (auto& e : it->second) {
      e.trace = b.trace;
      e.seq_id = seq_id;
      push(st, std::move(e));
    }
    // bindTrace 自身也是一个点（E2：header 解析完成）
    push(st, Event{b.trace, absl::string_view("hdr_decoded"), monoNs(mono), wallNs(wall), seq_id});
  }
  it->second.clear();
}

void rpcEvent(uint64_t conn_id, absl::string_view point, MonotonicTime mono) {
  if (!config().enabled) {
    return;
  }
  auto& st = tls();
  auto it = st.bindings.find(conn_id);
  // 未采样时这里就返回 —— 一次哈希查找加一次布尔判断。
  if (it == st.bindings.end() || !it->second.sampled) {
    return;
  }
  const auto& b = it->second;
  const int64_t m = monoNs(mono);
  // wall 由基准点推算，不再读 CLOCK_REALTIME
  push(st, Event{b.trace, point, m, b.base_wall + (m - b.base_mono), b.seq_id});
}

void rpcEventIfSampled(uint64_t conn_id, absl::string_view point, TimeSource& time_source) {
  if (!config().enabled) {
    return;
  }
  auto& st = tls();
  auto it = st.bindings.find(conn_id);
  if (it == st.bindings.end() || !it->second.sampled) {
    return;
  }

  const auto& b = it->second;
  const int64_t m = monoNs(time_source.monotonicTime());
  push(st, Event{b.trace, point, m, b.base_wall + (m - b.base_mono), b.seq_id});
}

void rpcEventTail(uint64_t conn_id, absl::string_view point, MonotonicTime mono, bool last) {
  if (!config().enabled) {
    return;
  }
  auto& st = tls();
  // 先查 finishing；若 endRpc 还没跑（事件循环先处理了写），回落到 bindings。
  auto it = st.finishing.find(conn_id);
  if (it == st.finishing.end()) {
    it = st.bindings.find(conn_id);
    if (it == st.bindings.end()) {
      return;
    }
  }
  const auto& b = it->second;
  if (b.trace == nullptr || !b.sampled) {
    return;
  }
  const int64_t m = monoNs(mono);
  push(st, Event{b.trace, point, m, b.base_wall + (m - b.base_mono), b.seq_id});
  if (last) {
    // 响应已真正写出，这条 trace 的尾巴到此为止。清掉，避免后续的空写
    // （onWriteReady 会重复触发）继续挂在它名下。
    st.finishing.erase(conn_id);
  }
}

bool isSampled(uint64_t conn_id) {
  if (!config().enabled) {
    return false;
  }
  auto& st = tls();
  auto it = st.bindings.find(conn_id);
  return it != st.bindings.end() && it->second.sampled;
}

void endRpc(uint64_t conn_id) {
  if (!config().enabled) {
    return;
  }
  auto& st = tls();
  // 把绑定移交给 finishing —— 下游响应的 writev 要到事件循环稍后才执行，
  // 那时 RPC 已经结束，但事件仍属于它。详见 ThreadState::finishing 的注释。
  auto it = st.bindings.find(conn_id);
  if (it != st.bindings.end()) {
    auto prev = st.finishing.find(conn_id);
    if (prev != st.finishing.end() && prev->second.trace != nullptr) {
      // 上一个响应还没写出就又结束了一个 —— 流水线。丢弃旧的并计数，
      // 不去猜哪次 writev 属于谁。
      g_write_lost.fetch_add(1, std::memory_order_relaxed);
    }
    st.finishing[conn_id] = it->second;
    st.bindings.erase(conn_id);
  }
  st.pending.erase(conn_id);
  // 槽位同样按连接清掉，否则长期运行下这个 map 会随连接数无界增长。
  // 时序上是安全的：下一个请求的 dn_epoll_wake 写在它自己的 bindTrace 之前，
  // 而 endRpc 属于上一个请求，两者不会交叠。
  st.slots.erase(conn_id);
  // 这里**不**落盘。
  //
  // 曾经每次 RPC 结束都 writeOut + fflush，那是为了解决「流量停止后
  // 最后一批事件永远留在内存」的问题 —— 但代价是每个采样请求一次
  // write 系统调用。收尾问题由线程退出时 ~ThreadState() 的刷盘解决，
  // 稳态路径不必为它买单。
}

void flush() {
  auto& st = tls();
  writeOut(st, st.ready);
  if (st.fp != nullptr) {
    std::fflush(st.fp);
  }
}

Stats stats() {
  return Stats{g_recorded.load(std::memory_order_relaxed),
               g_written.load(std::memory_order_relaxed),
               g_dropped.load(std::memory_order_relaxed)};
}

} // namespace KitexProbe
} // namespace Envoy
