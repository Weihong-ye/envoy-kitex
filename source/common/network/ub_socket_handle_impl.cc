#include "source/common/network/ub_socket_handle_impl.h"

#include <dlfcn.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/io_socket_error_impl.h"

namespace Envoy {
namespace Network {

namespace {

template <class Function> Function loadUbsocketSymbol(const char* name) {
  static_assert(sizeof(Function) == sizeof(void*));
  void* symbol = dlsym(RTLD_DEFAULT, name);
  Function function = nullptr;
  std::memcpy(&function, &symbol, sizeof(function)); // NOLINT(safe-memcpy)
  return function;
}

class UbsocketBufferFragment final : public Buffer::BufferFragment {
public:
  UbsocketBufferFragment(const Ubsocket::Api& api, Ubsocket::Iobuf* iobuf)
      : api_(api), iobuf_(iobuf), data_(api.iobuf_data_(iobuf)), size_(api.iobuf_size_(iobuf)) {}

  const void* data() const override { return data_; }
  size_t size() const override { return size_; }
  void done() override {
    api_.iobuf_release_(iobuf_);
    delete this;
  }

private:
  const Ubsocket::Api& api_;
  Ubsocket::Iobuf* const iobuf_;
  const void* const data_;
  const size_t size_;
};

class ScopedIobufs {
public:
  explicit ScopedIobufs(const Ubsocket::Api& api) : api_(api) {}
  ~ScopedIobufs() {
    for (Ubsocket::Iobuf* iobuf : iobufs_) {
      api_.iobuf_release_(iobuf);
    }
  }

  void add(Ubsocket::Iobuf* iobuf) { iobufs_.push_back(iobuf); }

private:
  const Ubsocket::Api& api_;
  std::vector<Ubsocket::Iobuf*> iobufs_;
};

} // namespace

namespace Ubsocket {

const Api& Api::instance() {
  static const Api api;
  return api;
}

Api::Api()
    : recv_zc_(loadUbsocketSymbol<RecvZcFn>("ubsocket_recv_zc")),
      iobuf_alloc_(loadUbsocketSymbol<IobufAllocFn>("ubsocket_iobuf_alloc")),
      chain_pop_front_(loadUbsocketSymbol<ChainPopFrontFn>("ubsocket_iobuf_chain_pop_front")),
      iobuf_data_(loadUbsocketSymbol<IobufDataFn>("ubsocket_iobuf_data")),
      iobuf_size_(loadUbsocketSymbol<IobufSizeFn>("ubsocket_iobuf_size")),
      iobuf_release_(loadUbsocketSymbol<IobufReleaseFn>("ubsocket_iobuf_release")),
      chain_release_(loadUbsocketSymbol<ChainReleaseFn>("ubsocket_iobuf_chain_release")),
      iobuf_query_(loadUbsocketSymbol<IobufQueryFn>("ubsocket_iobuf_query")),
      payload_capacity_(loadUbsocketSymbol<PayloadCapacityFn>("ubsocket_iobuf_payload_capacity")),
      writev_(loadUbsocketSymbol<WritevFn>("ubsocket_writev")),
      writev_copy_(loadUbsocketSymbol<WritevCopyFn>("ubsocket_writev_copy")),
      available_(recv_zc_ != nullptr && iobuf_alloc_ != nullptr && chain_pop_front_ != nullptr &&
                 iobuf_data_ != nullptr && iobuf_size_ != nullptr && iobuf_release_ != nullptr &&
                 chain_release_ != nullptr && iobuf_query_ != nullptr &&
                 payload_capacity_ != nullptr && writev_ != nullptr && writev_copy_ != nullptr) {}

} // namespace Ubsocket

Api::IoCallUint64Result UbSocketHandleImpl::read(Buffer::Instance& buffer,
                                                 std::optional<uint64_t> max_length_opt) {
  const Ubsocket::Api& api = Ubsocket::Api::instance();
  const uint64_t max_length = max_length_opt.value_or(std::numeric_limits<uint64_t>::max());
  if (max_length == 0) {
    return Api::ioCallUint64ResultNoError();
  }
  if (!api.available()) {
    return IoSocketHandleImpl::read(buffer, max_length_opt);
  }

  Ubsocket::RxResult result{};
  const size_t request_length = static_cast<size_t>(
      std::min<uint64_t>(max_length, static_cast<uint64_t>(std::numeric_limits<size_t>::max())));
  if (api.recv_zc_(fd_, request_length, &result) != 0) {
    return sysCallResultToIoCallResult(Api::SysCallSizeResult{-1, errno});
  }

  if (result.kind == Ubsocket::RxKind::FallbackRequired) {
    RELEASE_ASSERT(result.chain == nullptr && result.size == 0, "invalid UBSocket fallback result");
    return IoSocketHandleImpl::read(buffer, max_length_opt);
  }
  if (result.kind == Ubsocket::RxKind::Eof) {
    RELEASE_ASSERT(result.chain == nullptr && result.size == 0, "invalid UBSocket EOF result");
    return Api::ioCallUint64ResultNoError();
  }

  RELEASE_ASSERT(result.kind == Ubsocket::RxKind::Chain && result.chain != nullptr &&
                     result.size > 0,
                 "invalid UBSocket chain result");
  uint64_t appended = 0;
  Ubsocket::Iobuf* iobuf = nullptr;
  int pop_result = 0;
  while ((pop_result = api.chain_pop_front_(result.chain, &iobuf)) == 1) {
    auto* fragment = new UbsocketBufferFragment(api, iobuf);
    appended += fragment->size();
    buffer.addBufferFragment(*fragment);
  }
  api.chain_release_(result.chain);
  RELEASE_ASSERT(pop_result == 0, "failed to pop UBSocket receive chain");
  RELEASE_ASSERT(appended == static_cast<uint64_t>(result.size) && appended <= max_length,
                 "invalid UBSocket receive size");
  return Api::IoCallUint64Result(appended, Api::IoError::none());
}

Api::IoCallUint64Result UbSocketHandleImpl::writev(const Buffer::RawSlice* slices,
                                                   uint64_t num_slice) {
  constexpr size_t MaxIov = 1024;
  const Ubsocket::Api& api = Ubsocket::Api::instance();
  if (!api.available()) {
    return IoSocketHandleImpl::writev(slices, num_slice);
  }

  const auto write_copy = [&]() -> Api::IoCallUint64Result {
    std::vector<iovec> copy_iov;
    copy_iov.reserve(num_slice);
    for (uint64_t i = 0; i < num_slice; ++i) {
      if (slices[i].mem_ != nullptr && slices[i].len_ != 0) {
        copy_iov.push_back({slices[i].mem_, slices[i].len_});
      }
    }
    if (copy_iov.empty()) {
      return Api::ioCallUint64ResultNoError();
    }
    if (copy_iov.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
      return sysCallResultToIoCallResult(Api::SysCallSizeResult{-1, EINVAL});
    }
    const ssize_t rc = api.writev_copy_(fd_, copy_iov.data(), static_cast<int>(copy_iov.size()));
    return rc < 0 ? sysCallResultToIoCallResult(Api::SysCallSizeResult{-1, errno})
                  : Api::IoCallUint64Result(static_cast<uint64_t>(rc), Api::IoError::none());
  };

  const size_t payload_capacity = api.payload_capacity_();
  if (payload_capacity == 0) {
    return write_copy();
  }

  std::vector<iovec> iov;
  iov.reserve(num_slice);
  ScopedIobufs staged_iobufs(api);
  for (uint64_t i = 0; i < num_slice; ++i) {
    if (slices[i].mem_ == nullptr || slices[i].len_ == 0) {
      continue;
    }

    Ubsocket::Iobuf* queried = nullptr;
    size_t offset = 0;
    if (api.iobuf_query_(slices[i].mem_, slices[i].len_, &queried, &offset) == 0) {
      api.iobuf_release_(queried);
      iov.push_back({slices[i].mem_, slices[i].len_});
      if (iov.size() == MaxIov) {
        break;
      }
      continue;
    }

    const uint8_t* source = static_cast<const uint8_t*>(slices[i].mem_);
    size_t remaining = slices[i].len_;
    while (remaining != 0) {
      const size_t chunk = std::min(remaining, payload_capacity);
      Ubsocket::Iobuf* staged = nullptr;
      if (api.iobuf_alloc_(chunk, &staged) != 0) {
        return write_copy();
      }
      staged_iobufs.add(staged);
      void* destination = api.iobuf_data_(staged);
      std::memcpy(destination, source, chunk); // NOLINT(safe-memcpy)
      iov.push_back({destination, chunk});
      source += chunk;
      remaining -= chunk;
      if (iov.size() == MaxIov) {
        break;
      }
    }
    if (iov.size() == MaxIov) {
      break;
    }
  }

  if (iov.empty()) {
    return Api::ioCallUint64ResultNoError();
  }
  if (iov.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return sysCallResultToIoCallResult(Api::SysCallSizeResult{-1, EINVAL});
  }

  const ssize_t rc = api.writev_(fd_, iov.data(), static_cast<int>(iov.size()));
  return rc < 0 ? sysCallResultToIoCallResult(Api::SysCallSizeResult{-1, errno})
                : Api::IoCallUint64Result(static_cast<uint64_t>(rc), Api::IoError::none());
}

} // namespace Network
} // namespace Envoy
