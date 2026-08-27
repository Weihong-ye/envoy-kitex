#include "source/common/network/ub_socket_handle_impl.h"

#include <dlfcn.h>
#include <sys/ioctl.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <vector>

#include "envoy/common/exception.h"

#include "source/common/api/os_sys_calls_impl.h"
#include "source/common/common/logger.h"
#include "source/common/common/utility.h"
#include "source/common/network/io_socket_error_impl.h"

#include "absl/strings/string_view.h"

namespace Envoy {
namespace Network {
namespace {

constexpr size_t MaxUbsocketIov = 1024;
constexpr size_t MaxRxChainBlocks = 4096;
constexpr size_t MaxUdsReadBlocks = 8;

struct UdsReadStats {
  uint64_t calls{0};
  uint64_t read_bytes{0};
  uint64_t allocated_blocks{0};
  uint64_t used_blocks{0};
  uint64_t unused_blocks{0};
  uint64_t eagain_calls{0};
  uint64_t allocation_failures{0};
  uint64_t fionread_failures{0};
  bool logged_first_data{false};
};

UdsReadStats& udsReadStats() {
  static thread_local UdsReadStats stats;
  return stats;
}

void maybeLogUdsReadStats(UdsReadStats& stats, bool read_data, bool force = false) {
  const bool first_data = read_data && !stats.logged_first_data;
  if (!first_data && !force && !Ubsocket::diagnosticsStatsLogDue(stats.calls)) {
    return;
  }
  stats.logged_first_data = stats.logged_first_data || read_data;
  ENVOY_LOG_MISC(info,
                 "[UB_ZC_UDS_STATS] calls={} read_bytes={} allocated_blocks={} used_blocks={} "
                 "unused_blocks={} eagain_calls={} allocation_failures={} fionread_failures={}",
                 stats.calls, stats.read_bytes, stats.allocated_blocks, stats.used_blocks,
                 stats.unused_blocks, stats.eagain_calls, stats.allocation_failures,
                 stats.fionread_failures);
}

struct NetRxStats {
  uint64_t calls{0};
  uint64_t read_bytes{0};
  uint64_t returned_blocks{0};
  uint64_t attached_blocks{0};
  uint64_t attached_bytes{0};
  uint64_t max_chain_blocks{0};
  uint64_t eagain_calls{0};
  uint64_t anchor_allocation_failures{0};
  uint64_t invalid_chains{0};
  bool logged_first_data{false};
};

NetRxStats& netRxStats() {
  static thread_local NetRxStats stats;
  return stats;
}

void maybeLogNetRxStats(NetRxStats& stats, bool read_data, bool force = false) {
  const bool first_data = read_data && !stats.logged_first_data;
  if (!first_data && !force && !Ubsocket::diagnosticsStatsLogDue(stats.calls)) {
    return;
  }
  stats.logged_first_data = stats.logged_first_data || read_data;
  ENVOY_LOG_MISC(info,
                 "[UB_ZC_NET_RX_STATS] calls={} read_bytes={} returned_blocks={} "
                 "attached_blocks={} attached_bytes={} max_chain_blocks={} eagain_calls={} "
                 "anchor_allocation_failures={} invalid_chains={}",
                 stats.calls, stats.read_bytes, stats.returned_blocks, stats.attached_blocks,
                 stats.attached_bytes, stats.max_chain_blocks, stats.eagain_calls,
                 stats.anchor_allocation_failures, stats.invalid_chains);
}

struct OutputAllocationStats {
  uint64_t calls{0};
  uint64_t requested_bytes{0};
  uint64_t blocks{0};
  uint64_t failures{0};
  bool logged_first_allocation{false};
};

const char* outputBufferRoleName(Ubsocket::OutputBufferRole role) {
  switch (role) {
  case Ubsocket::OutputBufferRole::RouterRequest:
    return "router_request";
  case Ubsocket::OutputBufferRole::UpstreamTransport:
    return "upstream_transport";
  case Ubsocket::OutputBufferRole::ResponsePayload:
    return "response_payload";
  case Ubsocket::OutputBufferRole::ResponseFrame:
    return "response_frame";
  case Ubsocket::OutputBufferRole::Unspecified:
    return "unspecified";
  }
  return "unknown";
}

void maybeLogOutputFactoryDecision(Ubsocket::OutputBufferRole role, bool enabled,
                                   const char* reason, bool api_available,
                                   const char* transport_mode) {
  if (!Ubsocket::diagnosticsLoggingEnabled()) {
    return;
  }

  static std::atomic<uint32_t> logged_roles{0};
  const uint32_t role_bit = 1U << static_cast<uint32_t>(role);
  if ((logged_roles.fetch_or(role_bit, std::memory_order_relaxed) & role_bit) != 0) {
    return;
  }

  ENVOY_LOG_MISC(info,
                 "[UB_ZC_ALLOC_FACTORY] role={} enabled={} reason={} api_available={} "
                 "transport_mode={} block_total={} block_payload={}",
                 outputBufferRoleName(role), enabled, reason, api_available,
                 transport_mode != nullptr ? transport_mode : "unset", Ubsocket::BlockSize,
                 Ubsocket::BlockPayloadCapacity);
}

OutputAllocationStats& outputAllocationStats(Ubsocket::OutputBufferRole role) {
  static thread_local OutputAllocationStats stats[5];
  return stats[static_cast<size_t>(role)];
}

void recordOutputAllocation(Ubsocket::OutputBufferRole role, uint64_t requested_bytes,
                            bool success) {
  OutputAllocationStats& stats = outputAllocationStats(role);
  ++stats.calls;
  stats.requested_bytes += requested_bytes;
  if (success) {
    ++stats.blocks;
  } else {
    ++stats.failures;
  }
  const bool first_allocation = success && !stats.logged_first_allocation;
  if (!first_allocation && !Ubsocket::diagnosticsStatsLogDue(stats.calls) && success) {
    return;
  }
  stats.logged_first_allocation = stats.logged_first_allocation || success;
  ENVOY_LOG_MISC(info,
                 "[UB_ZC_ALLOC_STATS] role={} calls={} requested_bytes={} blocks={} failures={} "
                 "block_total={} block_payload={}",
                 outputBufferRoleName(role), stats.calls, stats.requested_bytes, stats.blocks,
                 stats.failures, Ubsocket::BlockSize, Ubsocket::BlockPayloadCapacity);
}

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
  block->flags = Ubsocket::BlockFlagsUb;
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

Api::IoCallUint64Result failClosedResult(int error, absl::string_view operation, os_fd_t fd) {
  ENVOY_LOG_MISC(error, "[UB_ZC_FAIL_CLOSED] {} rejected on fd {}: errno {} ({})", operation, fd,
                 error, errorDetails(error));
  return Api::IoCallUint64Result(/*rc=*/0, IoSocketError::create(error));
}

bool isAgainError(int error) { return error == EAGAIN || error == EWOULDBLOCK; }

Api::IoCallUint64Result againResult() {
  return Api::IoCallUint64Result(/*rc=*/0, IoSocketError::getIoSocketEagainError());
}

} // namespace

namespace Ubsocket {

bool diagnosticsEnabled() {
  static const bool enabled = []() {
    const char* value = std::getenv("UB_ZC_DIAGNOSTICS");
    return value != nullptr && (std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 ||
                                std::strcmp(value, "on") == 0);
  }();
  return enabled;
}

bool diagnosticsLoggingEnabled() {
  return diagnosticsEnabled() &&
         Logger::Registry::getLog(Logger::Id::misc).level() <= spdlog::level::info;
}

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
                 iobuf_deallocate_ != nullptr) {
  if (!diagnosticsLoggingEnabled()) {
    return;
  }

  Dl_info symbol_info{};
  void* readv_address = nullptr;
  static_assert(sizeof(readv_address) == sizeof(readv_));
  std::memcpy(&readv_address, &readv_, sizeof(readv_address)); // NOLINT(safe-memcpy)
  const char* library_path = "unresolved";
  if (readv_address != nullptr && dladdr(readv_address, &symbol_info) != 0 &&
      symbol_info.dli_fname != nullptr) {
    library_path = symbol_info.dli_fname;
  }
  const char* transport_mode = std::getenv("UBSOCKET_TRANS_MODE");
  const char* block_type = std::getenv("UBSOCKET_BLOCK_TYPE");
  ENVOY_LOG_MISC(info,
                 "[UB_ZC_BOOT] available={} readv={} writev={} allocator={} deallocator={} "
                 "transport_mode={} block_type={} block_total={} block_payload={} lib_path={}",
                 available_, readv_ != nullptr, writev_ != nullptr, iobuf_allocate_ != nullptr,
                 iobuf_deallocate_ != nullptr, transport_mode != nullptr ? transport_mode : "unset",
                 block_type != nullptr ? block_type : "unset", BlockSize, BlockPayloadCapacity,
                 library_path);
}

bool udsZeroCopyEnabled() {
  const char* transport_mode = std::getenv("UBSOCKET_TRANS_MODE");
  return Api::instance().available() && transport_mode != nullptr &&
         std::strcmp(transport_mode, "ub") == 0;
}

Buffer::OwnedImpl::SliceFactory createOutputSliceFactory(OutputBufferRole role) {
  const Api& api = Api::instance();
  const char* transport_mode = std::getenv("UBSOCKET_TRANS_MODE");
  const char* reason = "enabled";
  bool enabled = api.available();
  if (!api.available()) {
    reason = "api_unavailable";
  } else if (transport_mode == nullptr) {
    enabled = false;
    reason = "transport_mode_unset";
  } else if (std::strcmp(transport_mode, "ub") != 0) {
    enabled = false;
    reason = "transport_mode_not_ub";
  }

  maybeLogOutputFactoryDecision(role, enabled, reason, api.available(), transport_mode);
  if (!enabled) {
    return {};
  }

  return [&api, role](uint64_t min_capacity) -> Buffer::Slice {
    Block* block = allocateBlock(api, 0);
    if (block == nullptr) {
      if (diagnosticsLoggingEnabled()) {
        recordOutputAllocation(role, min_capacity, false);
      }
      ENVOY_LOG_MISC(error, "[UB_ZC_FAIL_CLOSED] UBSocket output slice allocation failed: {}",
                     errorDetails(ENOMEM));
      throw EnvoyException("UBSocket output slice allocation failed");
    }
    if (diagnosticsLoggingEnabled()) {
      recordOutputAllocation(role, min_capacity, true);
    }
    return Buffer::Slice(reinterpret_cast<uint8_t*>(block->data), BlockPayloadCapacity,
                         [&api, block]() { releaseBlock(api, block); });
  };
}

} // namespace Ubsocket

Api::IoCallUint64Result UbUnixSocketHandleImpl::read(Buffer::Instance& buffer,
                                                     std::optional<uint64_t> max_length_opt) {
  const uint64_t max_length = max_length_opt.value_or(std::numeric_limits<uint64_t>::max());
  if (max_length == 0) {
    return Api::ioCallUint64ResultNoError();
  }

  const Ubsocket::Api& api = Ubsocket::Api::instance();
  if (!api.available()) {
    return failClosedResult(ENOSYS, "UB UDS read symbols unavailable", fd_);
  }

  constexpr uint64_t MaxReadLength = MaxUdsReadBlocks * Ubsocket::BlockPayloadCapacity;
  const bool collect_stats = Ubsocket::diagnosticsLoggingEnabled();
  UdsReadStats* stats = collect_stats ? &udsReadStats() : nullptr;
  if (stats != nullptr) {
    ++stats->calls;
  }

  int pending_bytes = 0;
  const Api::SysCallIntResult pending_result = Api::OsSysCallsSingleton::get().ioctl(
      fd_, FIONREAD, &pending_bytes, sizeof(pending_bytes), nullptr, 0, nullptr);
  uint64_t desired_read_length = Ubsocket::BlockPayloadCapacity;
  if (pending_result.return_value_ == 0 && pending_bytes > 0) {
    desired_read_length =
        std::max<uint64_t>(desired_read_length, static_cast<uint64_t>(pending_bytes));
  } else if (pending_result.return_value_ != 0) {
    if (stats != nullptr) {
      ++stats->fionread_failures;
    }
  }

  const uint64_t read_length = std::min({max_length, MaxReadLength, desired_read_length});
  const size_t block_count = static_cast<size_t>(
      (read_length + Ubsocket::BlockPayloadCapacity - 1) / Ubsocket::BlockPayloadCapacity);

  std::vector<Ubsocket::Block*> blocks;
  std::vector<struct iovec> iov;
  blocks.reserve(block_count);
  iov.reserve(block_count);
  uint64_t remaining_capacity = read_length;
  for (size_t i = 0; i < block_count; ++i) {
    Ubsocket::Block* block = allocateBlock(api, 0);
    if (block == nullptr) {
      if (stats != nullptr) {
        stats->allocated_blocks += blocks.size();
        stats->unused_blocks += blocks.size();
        ++stats->allocation_failures;
      }
      for (Ubsocket::Block* allocated : blocks) {
        releaseBlock(api, allocated);
      }
      if (stats != nullptr) {
        maybeLogUdsReadStats(*stats, false, true);
      }
      return failClosedResult(ENOMEM, "UB UDS block allocation", fd_);
    }
    blocks.push_back(block);
    const size_t capacity =
        static_cast<size_t>(std::min<uint64_t>(remaining_capacity, Ubsocket::BlockPayloadCapacity));
    iov.push_back({block->data, capacity});
    remaining_capacity -= capacity;
  }
  if (stats != nullptr) {
    stats->allocated_blocks += block_count;
  }

  const Api::SysCallSizeResult syscall_result =
      Api::OsSysCallsSingleton::get().readv(fd_, iov.data(), static_cast<int>(iov.size()));
  if (syscall_result.return_value_ <= 0) {
    if (stats != nullptr) {
      stats->unused_blocks += block_count;
    }
    for (Ubsocket::Block* block : blocks) {
      releaseBlock(api, block);
    }
    if (syscall_result.return_value_ == 0) {
      return Api::ioCallUint64ResultNoError();
    }
    if (isAgainError(syscall_result.errno_)) {
      if (stats != nullptr) {
        ++stats->eagain_calls;
        maybeLogUdsReadStats(*stats, false);
      }
      return againResult();
    }
    if (stats != nullptr) {
      maybeLogUdsReadStats(*stats, false);
    }
    return failClosedResult(syscall_result.errno_, "UB UDS readv", fd_);
  }

  uint64_t remaining = static_cast<uint64_t>(syscall_result.return_value_);
  size_t used_blocks = 0;
  for (Ubsocket::Block* block : blocks) {
    const size_t used =
        static_cast<size_t>(std::min<uint64_t>(remaining, Ubsocket::BlockPayloadCapacity));
    if (used == 0) {
      releaseBlock(api, block);
      continue;
    }
    block->size = static_cast<uint32_t>(used);
    block->cap = static_cast<uint32_t>(used);
    auto* fragment = new UbsocketBufferFragment(api, block);
    buffer.addBufferFragment(*fragment);
    ++used_blocks;
    remaining -= used;
  }

  ASSERT(remaining == 0);
  if (stats != nullptr) {
    stats->read_bytes += static_cast<uint64_t>(syscall_result.return_value_);
    stats->used_blocks += used_blocks;
    stats->unused_blocks += block_count - used_blocks;
    maybeLogUdsReadStats(*stats, true);
    static std::atomic<bool> logged_active{false};
    if (!logged_active.exchange(true, std::memory_order_relaxed)) {
      ENVOY_LOG_MISC(info,
                     "[UB_ZC_ACTIVE] Envoy UDS RX is reading Kitex data directly into UBSocket "
                     "blocks (payload capacity {})",
                     Ubsocket::BlockPayloadCapacity);
    }
  }
  return Api::IoCallUint64Result(static_cast<uint64_t>(syscall_result.return_value_),
                                 Api::IoError::none());
}

Api::IoCallUint64Result UbSocketHandleImpl::read(Buffer::Instance& buffer,
                                                 std::optional<uint64_t> max_length_opt) {
  if (max_length_opt.value_or(std::numeric_limits<uint64_t>::max()) == 0) {
    return Api::ioCallUint64ResultNoError();
  }

  const Ubsocket::Api& api = Ubsocket::Api::instance();
  if (!api.available()) {
    return failClosedResult(ENOSYS, "ubsocket_readv symbols unavailable", fd_);
  }

  const bool collect_stats = Ubsocket::diagnosticsLoggingEnabled();
  NetRxStats* stats = collect_stats ? &netRxStats() : nullptr;
  if (stats != nullptr) {
    ++stats->calls;
  }

  Ubsocket::Block* anchor = allocateBlock(api, 0);
  if (anchor == nullptr) {
    if (stats != nullptr) {
      ++stats->anchor_allocation_failures;
      maybeLogNetRxStats(*stats, false, true);
    }
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
    if (isAgainError(saved_errno)) {
      if (stats != nullptr) {
        ++stats->eagain_calls;
        maybeLogNetRxStats(*stats, false);
      }
      return againResult();
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
    if (stats != nullptr) {
      ++stats->invalid_chains;
      maybeLogNetRxStats(*stats, false, true);
    }
    return failClosedResult(EPROTO, "invalid ubsocket_readv block chain", fd_);
  }

  anchor->u.next = nullptr;
  api.iobuf_deallocate_(anchor);
  for (Ubsocket::Block* received_block : blocks) {
    received_block->u.next = nullptr;
    auto* fragment = new UbsocketBufferFragment(api, received_block);
    buffer.addBufferFragment(*fragment);
  }

  if (stats != nullptr) {
    stats->read_bytes += static_cast<uint64_t>(rc);
    stats->returned_blocks += blocks.size();
    stats->attached_blocks += blocks.size();
    stats->attached_bytes += static_cast<uint64_t>(rc);
    stats->max_chain_blocks = std::max<uint64_t>(stats->max_chain_blocks, blocks.size());
    maybeLogNetRxStats(*stats, true);
  }

  static std::atomic<bool> logged_active{false};
  if (collect_stats && !logged_active.exchange(true, std::memory_order_relaxed)) {
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
  for (uint64_t i = 0; i < num_slice; ++i) {
    if (slices[i].mem_ != nullptr && slices[i].len_ != 0) {
      if (iov.size() == MaxUbsocketIov) {
        return failClosedResult(E2BIG, "ubsocket_writev iovec limit", fd_);
      }
      iov.push_back({slices[i].mem_, slices[i].len_});
    }
  }

  if (iov.empty()) {
    return Api::ioCallUint64ResultNoError();
  }

  const ssize_t rc = api.writev_(fd_, iov.data(), static_cast<int>(iov.size()));
  const int saved_errno = errno;
  if (rc < 0) {
    if (isAgainError(saved_errno)) {
      return againResult();
    }
    return failClosedResult(saved_errno, "ubsocket_writev", fd_);
  }

  if (Ubsocket::diagnosticsLoggingEnabled()) {
    static std::atomic<bool> logged_active{false};
    if (!logged_active.exchange(true, std::memory_order_relaxed)) {
      ENVOY_LOG_MISC(info,
                     "[UB_ZC_ACTIVE] Envoy TX is using mixed-memory ubsocket_writev; "
                     "UBSocket-owned slices are submitted directly (payload capacity {})",
                     Ubsocket::BlockPayloadCapacity);
    }
  }
  return Api::IoCallUint64Result(static_cast<uint64_t>(rc), Api::IoError::none());
}

} // namespace Network
} // namespace Envoy
