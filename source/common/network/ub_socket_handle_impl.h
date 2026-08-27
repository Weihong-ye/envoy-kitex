#pragma once

#include <sys/uio.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/io_socket_handle_impl.h"

namespace Envoy {
namespace Network {

// UBSocket intercepts AF_SMC (43) socket creation and keeps the POSIX control plane.
constexpr int UbsocketAddressFamily = 43;

namespace Ubsocket {

constexpr size_t BlockSize = 8 * 1024;

// PR9 exposes its zero-copy data path through the brpc-compatible block ABI. Keep this layout in
// sync with ock::ubs::Block and ub_bench_epoll_zc.cpp.
struct Block {
  std::atomic<int> nshared;
  uint16_t flags;
  uint16_t abi_check;
  uint32_t size;
  uint32_t cap;
  union {
    Block* next;
    uint64_t data_meta;
  } u;
  char* data;
};

static_assert(sizeof(Block) == 32, "UBSocket block ABI changed");
constexpr size_t BlockPayloadCapacity = BlockSize - sizeof(Block);
constexpr uint16_t BlockFlagsUb = 1U << 2;
static_assert(BlockSize == 8192, "UBSocket default RC_TP block size changed");
static_assert(BlockPayloadCapacity == 8160, "UBSocket default RC_TP payload size changed");

class Api {
public:
  using ReadvFn = ssize_t (*)(int, const struct iovec*, int);
  using WritevFn = ssize_t (*)(int, const struct iovec*, int);
  using IobufAllocateFn = void* (*)(size_t, const void*);
  using IobufDeallocateFn = void (*)(void*);

  static const Api& instance();

  bool available() const { return available_; }

  const ReadvFn readv_;
  const WritevFn writev_;
  const IobufAllocateFn iobuf_allocate_;
  const IobufDeallocateFn iobuf_deallocate_;

private:
  Api();

  const bool available_;
};

enum class OutputBufferRole : uint8_t {
  Unspecified,
  RouterRequest,
  UpstreamTransport,
  ResponsePayload,
  ResponseFrame,
};

// Diagnostics are opt-in so performance runs do not update hot-path counters merely because the
// process-wide logger permits INFO messages.
bool diagnosticsEnabled();
bool diagnosticsLoggingEnabled();

// Emit the first observation, one early stable snapshot, and then sparse cumulative summaries.
// Keep this fixed so UB_ZC_DIAGNOSTICS remains the only diagnostics control.
inline bool diagnosticsStatsLogDue(uint64_t count) {
  return count == 1 || count == 65536 || count % 262144 == 0;
}

// Reuse UBSocket's transport mode so the same binary keeps ordinary UDS behavior in TCP mode.
bool udsZeroCopyEnabled();

// Return an empty factory outside UB mode. In UB mode every new mutable slice is backed by one
// UBSocket block and retains the caller-owned block reference until the slice is drained.
Buffer::OwnedImpl::SliceFactory
createOutputSliceFactory(OutputBufferRole role = OutputBufferRole::Unspecified);

} // namespace Ubsocket

/**
 * Stream socket handle for the original PR9 UBSocket zero-copy ABI.
 *
 * RX attaches UBSocket-owned blocks to Envoy as BufferFragments. TX passes Envoy slices to the
 * mixed-memory ubsocket_writev entry point, which sends UBSocket blocks directly and stages only
 * ordinary memory. Any missing symbol or UBSocket TCP fallback is returned as an error instead of
 * silently using a POSIX path.
 */
class UbSocketHandleImpl final : public IoSocketHandleImpl {
public:
  explicit UbSocketHandleImpl(os_fd_t fd = INVALID_SOCKET, bool socket_v6only = false,
                              std::optional<int> domain = std::nullopt,
                              size_t address_cache_max_capacity = 0)
      : IoSocketHandleImpl(fd, socket_v6only, domain, address_cache_max_capacity) {}

  Api::IoCallUint64Result read(Buffer::Instance& buffer,
                               std::optional<uint64_t> max_length) override;
  Api::IoCallUint64Result writev(const Buffer::RawSlice* slices, uint64_t num_slice) override;
};

/**
 * AF_UNIX handle that reads Kitex UDS payloads directly into UBSocket-owned blocks.
 */
class UbUnixSocketHandleImpl final : public IoSocketHandleImpl {
public:
  explicit UbUnixSocketHandleImpl(os_fd_t fd = INVALID_SOCKET, bool socket_v6only = false,
                                  std::optional<int> domain = std::nullopt,
                                  size_t address_cache_max_capacity = 0)
      : IoSocketHandleImpl(fd, socket_v6only, domain, address_cache_max_capacity) {}

  Api::IoCallUint64Result read(Buffer::Instance& buffer,
                               std::optional<uint64_t> max_length) override;
};

} // namespace Network
} // namespace Envoy
