#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "envoy/common/exception.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/ub_socket_handle_impl.h"

#include "test/test_common/environment.h"

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
  bool fail_alloc{false};
  int last_write_iovcnt{0};
  std::vector<void*> last_write_iov_bases;
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
  void SetUp() override {
    fake_ubsocket_state = &state_;
    const char* transport_mode = std::getenv("UBSOCKET_TRANS_MODE");
    if (transport_mode != nullptr) {
      original_transport_mode_ = transport_mode;
    }
  }

  void TearDown() override {
    for (void* allocation : state_.allocations) {
      ::operator delete[](allocation);
    }
    fake_ubsocket_state = nullptr;
    if (original_transport_mode_.has_value()) {
      TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", *original_transport_mode_, true);
    } else {
      TestEnvironment::unsetEnvVar("UBSOCKET_TRANS_MODE");
    }
  }

  FakeUbsocketState state_;
  std::optional<std::string> original_transport_mode_;
};

TEST(UbSocketDiagnosticsTest, UsesSparseStableLogMilestones) {
  EXPECT_TRUE(Ubsocket::diagnosticsStatsLogDue(1));
  EXPECT_TRUE(Ubsocket::diagnosticsStatsLogDue(65536));
  EXPECT_TRUE(Ubsocket::diagnosticsStatsLogDue(262144));
  EXPECT_TRUE(Ubsocket::diagnosticsStatsLogDue(524288));
  EXPECT_FALSE(Ubsocket::diagnosticsStatsLogDue(2));
  EXPECT_FALSE(Ubsocket::diagnosticsStatsLogDue(4096));
  EXPECT_FALSE(Ubsocket::diagnosticsStatsLogDue(65537));
}

TEST_F(UbSocketHandleImplTest, EnablesUdsZeroCopyOnlyForExistingUbTransportMode) {
  TestEnvironment::unsetEnvVar("UBSOCKET_TRANS_MODE");
  EXPECT_FALSE(Ubsocket::udsZeroCopyEnabled());

  TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", "tcp", true);
  EXPECT_FALSE(Ubsocket::udsZeroCopyEnabled());

  TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", "ub", true);
  EXPECT_TRUE(Ubsocket::udsZeroCopyEnabled());

  TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", "UB", true);
  EXPECT_FALSE(Ubsocket::udsZeroCopyEnabled());
}

TEST_F(UbSocketHandleImplTest, OutputSliceFactoryIsEmptyOutsideUbMode) {
  TestEnvironment::unsetEnvVar("UBSOCKET_TRANS_MODE");
  EXPECT_FALSE(Ubsocket::createOutputSliceFactory());

  TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", "tcp", true);
  EXPECT_FALSE(Ubsocket::createOutputSliceFactory());
}

TEST_F(UbSocketHandleImplTest, OutputSliceFactoryFillsBlocksAndMovesWithoutCopying) {
  TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", "ub", true);
  Buffer::OwnedImpl buffer(Ubsocket::createOutputSliceFactory());
  const std::string first = "small thrift fields";
  const std::string payload(2 * Ubsocket::BlockPayloadCapacity, 'p');

  buffer.add(first);
  buffer.add(payload);

  EXPECT_EQ(first.size() + payload.size(), buffer.length());
  EXPECT_EQ(3, state_.alloc_calls);
  const Buffer::RawSliceVector before_move = buffer.getRawSlices();
  ASSERT_EQ(3, before_move.size());
  EXPECT_EQ(Ubsocket::BlockPayloadCapacity, before_move[0].len_);
  EXPECT_EQ(Ubsocket::BlockPayloadCapacity, before_move[1].len_);
  EXPECT_EQ(first.size(), before_move[2].len_);

  Buffer::OwnedImpl destination;
  destination.move(buffer);

  EXPECT_EQ(0, buffer.length());
  EXPECT_EQ(first + payload, destination.toString());
  const Buffer::RawSliceVector after_move = destination.getRawSlices();
  ASSERT_EQ(before_move.size(), after_move.size());
  for (size_t i = 0; i < before_move.size(); ++i) {
    EXPECT_EQ(before_move[i].mem_, after_move[i].mem_);
  }
  EXPECT_EQ(0, state_.dealloc_calls);

  destination.drain(destination.length());
  EXPECT_EQ(3, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, OutputSliceAllocationFailsClosed) {
  TestEnvironment::setEnvVar("UBSOCKET_TRANS_MODE", "ub", true);
  state_.fail_alloc = true;
  Buffer::OwnedImpl buffer(Ubsocket::createOutputSliceFactory());

  EXPECT_THROW(buffer.add("request"), EnvoyException);
  EXPECT_EQ(0, buffer.length());
  EXPECT_EQ(1, state_.alloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

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

TEST_F(UbSocketHandleImplTest, ReturnsAgainForTransientReadAndReleasesAnchor) {
  state_.read_error = EAGAIN;
  Buffer::OwnedImpl buffer;
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.read(buffer, 1024);

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(Api::IoError::IoErrorCode::Again, result.err_->getErrorCode());
  EXPECT_EQ(EAGAIN, result.err_->getSystemErrorCode());
  EXPECT_EQ(1, state_.alloc_calls);
  EXPECT_EQ(1, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
  EXPECT_EQ(0, buffer.length());
}

TEST_F(UbSocketHandleImplTest, PassesHeapSlicesToMixedMemoryWritevWithoutEnvoyStaging) {
  std::string first = "hello";
  std::string second = " world";
  Buffer::RawSlice slices[] = {{first.data(), first.size()}, {second.data(), second.size()}};
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.writev(slices, 2);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(11, result.return_value_);
  EXPECT_EQ("hello world", state_.written);
  ASSERT_EQ(2, state_.last_write_iovcnt);
  EXPECT_EQ(first.data(), state_.last_write_iov_bases[0]);
  EXPECT_EQ(second.data(), state_.last_write_iov_bases[1]);
  EXPECT_EQ(0, state_.alloc_calls);
  EXPECT_EQ(0, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, LeavesLargeSliceSplittingToUbsocket) {
  std::string data(16 * 1024, 'a');
  Buffer::RawSlice slice{data.data(), data.size()};
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.writev(&slice, 1);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(data.size(), result.return_value_);
  EXPECT_EQ(data, state_.written);
  EXPECT_EQ(1, state_.last_write_iovcnt);
  EXPECT_EQ(data.data(), state_.last_write_iov_bases[0]);
  EXPECT_EQ(0, state_.alloc_calls);
  EXPECT_EQ(0, state_.dealloc_calls);
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
  EXPECT_EQ(0, state_.dealloc_calls);
  EXPECT_TRUE(state_.written.empty());
}

TEST_F(UbSocketHandleImplTest, ReturnsAgainForTransientMixedMemoryWrite) {
  state_.write_error = EAGAIN;
  std::string data = "retry on the next write event";
  Buffer::RawSlice slice{data.data(), data.size()};
  UbSocketHandleImpl handle(42, false, UbsocketAddressFamily);

  Api::IoCallUint64Result result = handle.writev(&slice, 1);

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(Api::IoError::IoErrorCode::Again, result.err_->getErrorCode());
  EXPECT_EQ(EAGAIN, result.err_->getSystemErrorCode());
  EXPECT_EQ(0, state_.alloc_calls);
  EXPECT_EQ(0, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
  EXPECT_TRUE(state_.written.empty());
}

TEST_F(UbSocketHandleImplTest, ReadsUnixSocketDirectlyIntoUbsocketFragment) {
  int fds[2];
  ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds));
  const std::string data = "kitex payload";
  ASSERT_EQ(static_cast<ssize_t>(data.size()), ::write(fds[0], data.data(), data.size()));

  Buffer::OwnedImpl buffer;
  {
    UbUnixSocketHandleImpl handle(fds[1], false, AF_UNIX);
    Api::IoCallUint64Result result = handle.read(buffer, data.size());

    ASSERT_TRUE(result.ok());
    EXPECT_EQ(data.size(), result.return_value_);
    EXPECT_EQ(data, buffer.toString());
    EXPECT_EQ(1, state_.alloc_calls);
    EXPECT_EQ(0, state_.dealloc_calls);
  }
  EXPECT_EQ(0, ::close(fds[0]));

  buffer.drain(buffer.length());
  EXPECT_EQ(1, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, SizesUnboundedUnixReadFromPendingBytes) {
  int fds[2];
  ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds));
  const std::string data(1024, 'u');
  ASSERT_EQ(static_cast<ssize_t>(data.size()), ::write(fds[0], data.data(), data.size()));

  Buffer::OwnedImpl buffer;
  {
    UbUnixSocketHandleImpl handle(fds[1], false, AF_UNIX);
    Api::IoCallUint64Result result = handle.read(buffer, std::nullopt);

    ASSERT_TRUE(result.ok());
    EXPECT_EQ(data.size(), result.return_value_);
    EXPECT_EQ(data, buffer.toString());
    EXPECT_EQ(1, state_.alloc_calls);
    EXPECT_EQ(0, state_.dealloc_calls);
  }
  EXPECT_EQ(0, ::close(fds[0]));

  buffer.drain(buffer.length());
  EXPECT_EQ(1, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, RoundsPendingUnixBytesUpToRequiredBlocks) {
  int fds[2];
  ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds));
  const std::string data(20 * 1024, 'm');
  ASSERT_EQ(static_cast<ssize_t>(data.size()), ::write(fds[0], data.data(), data.size()));

  Buffer::OwnedImpl buffer;
  {
    UbUnixSocketHandleImpl handle(fds[1], false, AF_UNIX);
    Api::IoCallUint64Result result = handle.read(buffer, std::nullopt);

    ASSERT_TRUE(result.ok());
    EXPECT_EQ(data.size(), result.return_value_);
    EXPECT_EQ(data, buffer.toString());
    EXPECT_EQ(3, state_.alloc_calls);
    EXPECT_EQ(0, state_.dealloc_calls);
  }
  EXPECT_EQ(0, ::close(fds[0]));

  buffer.drain(buffer.length());
  EXPECT_EQ(3, state_.dealloc_calls);
  EXPECT_TRUE(state_.allocations.empty());
}

TEST_F(UbSocketHandleImplTest, UnboundedUnixReadAllocatesOneBlockBeforeAgain) {
  int fds[2];
  ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds));

  Buffer::OwnedImpl buffer;
  {
    UbUnixSocketHandleImpl handle(fds[1], false, AF_UNIX);
    Api::IoCallUint64Result result = handle.read(buffer, std::nullopt);

    ASSERT_FALSE(result.ok());
    EXPECT_EQ(Api::IoError::IoErrorCode::Again, result.err_->getErrorCode());
    EXPECT_EQ(EAGAIN, result.err_->getSystemErrorCode());
    EXPECT_EQ(1, state_.alloc_calls);
    EXPECT_EQ(1, state_.dealloc_calls);
    EXPECT_TRUE(state_.allocations.empty());
  }
  EXPECT_EQ(0, ::close(fds[0]));
}

} // namespace
} // namespace Network
} // namespace Envoy

extern "C" void* ubsocket_iobuf_allocate(size_t size, const void*) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  ++state.alloc_calls;
  if (state.fail_alloc) {
    errno = ENOMEM;
    return nullptr;
  }
  if (size != Envoy::Network::Ubsocket::BlockSize) {
    errno = ENOMEM;
    return nullptr;
  }
  void* raw = ::operator new[](size);
  state.allocations.insert(raw);
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

  state.last_write_iovcnt = iovcnt;
  state.last_write_iov_bases.clear();
  ssize_t total = 0;
  for (int i = 0; i < iovcnt; ++i) {
    state.last_write_iov_bases.push_back(iov[i].iov_base);
    state.written.append(static_cast<const char*>(iov[i].iov_base), iov[i].iov_len);
    total += static_cast<ssize_t>(iov[i].iov_len);
  }
  return total;
}
