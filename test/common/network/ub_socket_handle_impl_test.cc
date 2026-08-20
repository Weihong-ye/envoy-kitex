#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/ub_socket_handle_impl.h"

#include "absl/strings/string_view.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace Network {
namespace {

struct FakeUbsocketState {
  std::vector<std::string> rx_chunks;
  std::unordered_set<void*> allocations;
  std::string written;
  int read_error{0};
  int write_error{0};
  int alloc_calls{0};
  int dealloc_calls{0};
};

FakeUbsocketState* fake_ubsocket_state;

Ubsocket::Block* makeBlock(absl::string_view contents) {
  void* raw = ::operator new[](Ubsocket::BlockSize);
  fake_ubsocket_state->allocations.insert(raw);
  ++fake_ubsocket_state->alloc_calls;
  auto* block = static_cast<Ubsocket::Block*>(raw);
  block->nshared.store(1, std::memory_order_relaxed);
  block->flags = 0;
  block->abi_check = 0;
  block->size = static_cast<uint32_t>(contents.size());
  block->cap = static_cast<uint32_t>(contents.size());
  block->u.next = nullptr;
  block->data = static_cast<char*>(raw) + sizeof(Ubsocket::Block);
  std::memcpy(block->data, contents.data(), contents.size()); // NOLINT(safe-memcpy)
  return block;
}

class UbSocketHandleImplTest : public testing::Test {
protected:
  void SetUp() override { fake_ubsocket_state = &state_; }

  void TearDown() override {
    for (void* allocation : state_.allocations) {
      ::operator delete[](allocation);
    }
    fake_ubsocket_state = nullptr;
  }

  FakeUbsocketState state_;
};

TEST_F(UbSocketHandleImplTest, AddsPr9BlocksAsFragmentsAndReleasesOnDrain) {
  state_.rx_chunks = {"hello", " world"};
  Buffer::OwnedImpl buffer;
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.read(buffer, 4);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(11, result.return_value_);
  EXPECT_EQ("hello world", buffer.toString());
  EXPECT_EQ(1, state_.dealloc_calls); // RX anchor only.

  buffer.drain(buffer.length());
  EXPECT_EQ(3, state_.dealloc_calls); // Anchor and two received blocks.
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, PropagatesFailClosedReadWithoutPosixFallback) {
  state_.read_error = EPROTONOSUPPORT;
  Buffer::OwnedImpl buffer;
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.read(buffer, 1024);

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(EPROTONOSUPPORT, result.err_->getSystemErrorCode());
  EXPECT_EQ(1, state_.dealloc_calls);
  EXPECT_EQ(0, buffer.length());
}

TEST_F(UbSocketHandleImplTest, StagesHeapSlicesAndCallsExplicitWritev) {
  std::string first = "hello";
  std::string second = " world";
  Buffer::RawSlice slices[] = {{first.data(), first.size()}, {second.data(), second.size()}};
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.writev(slices, 2);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(11, result.return_value_);
  EXPECT_EQ("hello world", state_.written);
  EXPECT_EQ(2, state_.alloc_calls);
  EXPECT_EQ(2, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, PropagatesFailClosedWriteWithoutPosixFallback) {
  state_.write_error = EPROTONOSUPPORT;
  std::string data = "fallback must fail";
  Buffer::RawSlice slice{data.data(), data.size()};
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.writev(&slice, 1);

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(EPROTONOSUPPORT, result.err_->getSystemErrorCode());
  EXPECT_EQ(1, state_.dealloc_calls);
  EXPECT_TRUE(state_.written.empty());
}

} // namespace
} // namespace Network
} // namespace Envoy

extern "C" void* ubsocket_iobuf_allocate(size_t size, const void*) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  if (size != Envoy::Network::Ubsocket::BlockSize) {
    errno = ENOMEM;
    return nullptr;
  }
  void* raw = ::operator new[](size);
  state.allocations.insert(raw);
  ++state.alloc_calls;
  return raw;
}

extern "C" void ubsocket_iobuf_deallocate(void* raw) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  if (state.allocations.erase(raw) == 1) {
    ++state.dealloc_calls;
    ::operator delete[](raw);
  }
}

extern "C" ssize_t ubsocket_readv(int, const struct iovec* iov, int iovcnt) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  if (state.read_error != 0) {
    errno = state.read_error;
    return -1;
  }
  if (iovcnt != 1) {
    errno = EINVAL;
    return -1;
  }

  using Envoy::Network::Ubsocket::Block;
  auto* anchor = reinterpret_cast<Block*>(static_cast<char*>(iov[0].iov_base) - sizeof(Block));
  Block* tail = anchor;
  ssize_t total = 0;
  for (const std::string& chunk : state.rx_chunks) {
    Block* block = Envoy::Network::makeBlock(chunk);
    tail->u.next = block;
    tail = block;
    total += static_cast<ssize_t>(chunk.size());
  }
  return total;
}

extern "C" ssize_t ubsocket_writev(int, const struct iovec* iov, int iovcnt) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  if (state.write_error != 0) {
    errno = state.write_error;
    return -1;
  }

  ssize_t total = 0;
  for (int i = 0; i < iovcnt; ++i) {
    using Envoy::Network::Ubsocket::Block;
    auto* block = reinterpret_cast<Block*>(static_cast<char*>(iov[i].iov_base) - sizeof(Block));
    if (state.allocations.count(block) == 0) {
      errno = EFAULT;
      return -1;
    }
    state.written.append(static_cast<const char*>(iov[i].iov_base), iov[i].iov_len);
    total += static_cast<ssize_t>(iov[i].iov_len);
  }
  return total;
}
