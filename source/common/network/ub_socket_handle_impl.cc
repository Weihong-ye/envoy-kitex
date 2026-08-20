#include "source/common/network/ub_socket_handle_impl.h"

#include <dlfcn.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <vector>

#include "source/common/common/logger.h"
#include "source/common/common/utility.h"
#include "source/common/network/io_socket_error_impl.h"

#include "absl/strings/string_view.h"

namespace Envoy {
namespace Network {
namespace {

constexpr size_t MaxUbsocketIov = 1024;
constexpr size_t MaxRxChainBlocks = 4096;

template <class Function> Function loadUbsocketSymbol(const char* name) {
  static_assert(sizeof(Function) == sizeof(void*));
  void* symbol = dlsym(RTLD_DEFAULT, name);
  Function function = nullptr;
  std::memcpy(&function, &symbol, sizeof(function)); // NOLINT(safe-memcpy)
  return function;
}

void releaseBlock(const Ubsocket::Api& api, Ubsocket::Block* block) {
  if (block->nshared.fetch_sub(1, std::memory_order_release) == 1) {
    std::atomic_thread_fence(std::memory_order_acquire);
    api.iobuf_deallocate_(block);
  }
}

void releaseChain(const Ubsocket::Api& api, Ubsocket::Block* block) {
  size_t count = 0;
  while (block != nullptr && count++ < MaxRxChainBlocks) {
    Ubsocket::Block* next = block->u.next;
    block->u.next = nullptr;
    releaseBlock(api, block);
    block = next;
  }
}

Ubsocket::Block* allocateBlock(const Ubsocket::Api& api, size_t data_size) {
  void* raw = api.iobuf_allocate_(Ubsocket::BlockSize, nullptr);
  if (raw == nullptr) {
    return nullptr;
  }

  auto* block = static_cast<Ubsocket::Block*>(raw);
  block->nshared.store(1, std::memory_order_relaxed);
  block->flags = 0;
  block->abi_check = 0;
  block->size = static_cast<uint32_t>(data_size);
  block->cap = static_cast<uint32_t>(Ubsocket::BlockPayloadCapacity);
  block->u.next = nullptr;
  block->data = static_cast<char*>(raw) + sizeof(Ubsocket::Block);
  return block;
}

class UbsocketBufferFragment final : public Buffer::BufferFragment {
public:
  UbsocketBufferFragment(const Ubsocket::Api& api, Ubsocket::Block* block)
      : api_(api), block_(block), data_(block->data), size_(block->cap) {}

  const void* data() const override { return data_; }
  size_t size() const override { return size_; }

  void done() override {
    releaseBlock(api_, block_);
    delete this;
  }

private:
  const Ubsocket::Api& api_;
  Ubsocket::Block* const block_;
  const void* const data_;
  const size_t size_;
};

class ScopedBlocks {
public:
  explicit ScopedBlocks(const Ubsocket::Api& api) : api_(api) {}
  ~ScopedBlocks() {
    for (Ubsocket::Block* block : blocks_) {
      releaseBlock(api_, block);
    }
  }

  void add(Ubsocket::Block* block) { blocks_.push_back(block); }

private:
  const Ubsocket::Api& api_;
  std::vector<Ubsocket::Block*> blocks_;
};

Api::IoCallUint64Result failClosedResult(int error, absl::string_view operation, os_fd_t fd) {
  ENVOY_LOG_MISC(error, "[UB_ZC_FAIL_CLOSED] {} rejected on fd {}: errno {} ({})", operation, fd,
                 error, errorDetails(error));
  return Api::IoCallUint64Result(/*rc=*/0, IoSocketError::create(error));
}

} // namespace

namespace Ubsocket {

const Api& Api::instance() {
  static const Api api;
  return api;
}

Api::Api()
    : readv_(loadUbsocketSymbol<ReadvFn>("ubsocket_readv")),
      writev_(loadUbsocketSymbol<WritevFn>("ubsocket_writev")),
      iobuf_allocate_(loadUbsocketSymbol<IobufAllocateFn>("ubsocket_iobuf_allocate")),
      iobuf_deallocate_(loadUbsocketSymbol<IobufDeallocateFn>("ubsocket_iobuf_deallocate")),
      available_(readv_ != nullptr && writev_ != nullptr && iobuf_allocate_ != nullptr &&
                 iobuf_deallocate_ != nullptr) {}

} // namespace Ubsocket

Api::IoCallUint64Result UbSocketHandleImpl::read(Buffer::Instance& buffer,
                                                 std::optional<uint64_t> max_length_opt) {
  if (max_length_opt.value_or(std::numeric_limits<uint64_t>::max()) == 0) {
    return Api::ioCallUint64ResultNoError();
  }

  const Ubsocket::Api& api = Ubsocket::Api::instance();
  if (!api.available()) {
    return failClosedResult(ENOSYS, "ubsocket_readv symbols unavailable", fd_);
  }

  Ubsocket::Block* anchor = allocateBlock(api, 0);
  if (anchor == nullptr) {
    return failClosedResult(ENOMEM, "ubsocket RX anchor allocation", fd_);
  }

  struct iovec iov = {anchor->data, Ubsocket::BlockPayloadCapacity};
  const ssize_t rc = api.readv_(fd_, &iov, 1);
  const int saved_errno = errno;
  if (rc <= 0) {
    api.iobuf_deallocate_(anchor);
    if (rc == 0) {
      return Api::ioCallUint64ResultNoError();
    }
    return failClosedResult(saved_errno, "ubsocket_readv", fd_);
  }

  std::vector<Ubsocket::Block*> blocks;
  Ubsocket::Block* block = anchor->u.next;
  uint64_t received = 0;
  bool valid = block != nullptr;
  while (valid && block != nullptr && received < static_cast<uint64_t>(rc) &&
         blocks.size() < MaxRxChainBlocks) {
    const bool valid_block =
        block->data == reinterpret_cast<char*>(block) + sizeof(Ubsocket::Block) &&
        block->cap != 0 && block->cap <= Ubsocket::BlockPayloadCapacity &&
        received + block->cap <= static_cast<uint64_t>(rc);
    if (!valid_block) {
      valid = false;
      break;
    }
    blocks.push_back(block);
    received += block->cap;
    block = block->u.next;
  }
  valid = valid && received == static_cast<uint64_t>(rc) && block == nullptr;

  if (!valid) {
    Ubsocket::Block* chain = anchor->u.next;
    anchor->u.next = nullptr;
    releaseChain(api, chain);
    api.iobuf_deallocate_(anchor);
    return failClosedResult(EPROTO, "invalid ubsocket_readv block chain", fd_);
  }

  anchor->u.next = nullptr;
  api.iobuf_deallocate_(anchor);
  for (Ubsocket::Block* received_block : blocks) {
    received_block->u.next = nullptr;
    auto* fragment = new UbsocketBufferFragment(api, received_block);
    buffer.addBufferFragment(*fragment);
  }

  static std::atomic<bool> logged_active{false};
  if (!logged_active.exchange(true, std::memory_order_relaxed)) {
    ENVOY_LOG_MISC(info,
                   "[UB_ZC_ACTIVE] Envoy RX is using explicit ubsocket_readv block fragments "
                   "(payload capacity {})",
                   Ubsocket::BlockPayloadCapacity);
  }
  return Api::IoCallUint64Result(static_cast<uint64_t>(rc), Api::IoError::none());
}

Api::IoCallUint64Result UbSocketHandleImpl::writev(const Buffer::RawSlice* slices,
                                                   uint64_t num_slice) {
  const Ubsocket::Api& api = Ubsocket::Api::instance();
  if (!api.available()) {
    return failClosedResult(ENOSYS, "ubsocket_writev symbols unavailable", fd_);
  }

  std::vector<struct iovec> iov;
  iov.reserve(std::min<uint64_t>(num_slice, MaxUbsocketIov));
  ScopedBlocks staged_blocks(api);
  for (uint64_t i = 0; i < num_slice; ++i) {
    const auto* source = static_cast<const uint8_t*>(slices[i].mem_);
    size_t remaining = slices[i].len_;
    while (source != nullptr && remaining != 0) {
      if (iov.size() == MaxUbsocketIov) {
        return failClosedResult(E2BIG, "ubsocket_writev iovec limit", fd_);
      }
      const size_t chunk = std::min(remaining, Ubsocket::BlockPayloadCapacity);
      Ubsocket::Block* block = allocateBlock(api, chunk);
      if (block == nullptr) {
        return failClosedResult(ENOMEM, "ubsocket TX block allocation", fd_);
      }
      staged_blocks.add(block);
      std::memcpy(block->data, source, chunk); // NOLINT(safe-memcpy)
      iov.push_back({block->data, chunk});
      source += chunk;
      remaining -= chunk;
    }
  }

  if (iov.empty()) {
    return Api::ioCallUint64ResultNoError();
  }

  const ssize_t rc = api.writev_(fd_, iov.data(), static_cast<int>(iov.size()));
  const int saved_errno = errno;
  if (rc < 0) {
    return failClosedResult(saved_errno, "ubsocket_writev", fd_);
  }

  static std::atomic<bool> logged_active{false};
  if (!logged_active.exchange(true, std::memory_order_relaxed)) {
    ENVOY_LOG_MISC(info,
                   "[UB_ZC_ACTIVE] Envoy TX is using explicit ubsocket_writev with UBSocket-owned "
                   "staging blocks (payload capacity {})",
                   Ubsocket::BlockPayloadCapacity);
  }
  return Api::IoCallUint64Result(static_cast<uint64_t>(rc), Api::IoError::none());
}

} // namespace Network
} // namespace Envoy
