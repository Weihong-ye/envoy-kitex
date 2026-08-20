#pragma once

#include <sys/uio.h>

#include "source/common/network/io_socket_handle_impl.h"

namespace Envoy {
namespace Network {

constexpr int UbsocketAddressFamily = 43;

namespace Ubsocket {

struct Iobuf;
struct IobufChain;

enum class RxKind : int {
  Chain = 0,
  FallbackRequired = 1,
  Eof = 2,
};

struct RxResult {
  ssize_t size;
  RxKind kind;
  IobufChain* chain;
};

class Api {
public:
  using RecvZcFn = int (*)(int, size_t, RxResult*);
  using IobufAllocFn = int (*)(size_t, Iobuf**);
  using ChainPopFrontFn = int (*)(IobufChain*, Iobuf**);
  using IobufDataFn = void* (*)(Iobuf*);
  using IobufSizeFn = size_t (*)(const Iobuf*);
  using IobufReleaseFn = void (*)(Iobuf*);
  using ChainReleaseFn = void (*)(IobufChain*);
  using IobufQueryFn = int (*)(const void*, size_t, Iobuf**, size_t*);
  using PayloadCapacityFn = size_t (*)(void);
  using WritevFn = ssize_t (*)(int, const struct iovec*, int);
  using WritevCopyFn = ssize_t (*)(int, const struct iovec*, int);

  static const Api& instance();

  bool available() const { return available_; }

  const RecvZcFn recv_zc_;
  const IobufAllocFn iobuf_alloc_;
  const ChainPopFrontFn chain_pop_front_;
  const IobufDataFn iobuf_data_;
  const IobufSizeFn iobuf_size_;
  const IobufReleaseFn iobuf_release_;
  const ChainReleaseFn chain_release_;
  const IobufQueryFn iobuf_query_;
  const PayloadCapacityFn payload_capacity_;
  const WritevFn writev_;
  const WritevCopyFn writev_copy_;

private:
  Api();

  const bool available_;
};

} // namespace Ubsocket

/**
 * Stream socket handle that consumes UBSocket-owned RX blocks directly and exposes them to Envoy
 * as immutable BufferFragments. TX slices already backed by UBSocket memory are sent directly;
 * other slices are staged into UBSocket iobufs before the explicit zero-copy writev call.
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
