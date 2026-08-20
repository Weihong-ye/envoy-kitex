#pragma once

#include <sys/uio.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "source/common/network/io_socket_handle_impl.h"

namespace Envoy {
namespace Network {

// UBSocket intercepts AF_SMC (43) socket creation and keeps the POSIX control plane.
constexpr int UbsocketAddressFamily = 43;

namespace Ubsocket {

constexpr size_t BlockSize = 64 * 1024;

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

} // namespace Ubsocket

/**
 * Stream socket handle for the original PR9 UBSocket zero-copy ABI.
 *
 * RX attaches UBSocket-owned blocks to Envoy as BufferFragments. TX stages ordinary Envoy slices
 * into UBSocket blocks and calls the explicit ubsocket_writev entry point. Any missing symbol or
 * UBSocket TCP fallback is returned as an error instead of silently using a copy/POSIX path.
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

} // namespace Network
} // namespace Envoy
