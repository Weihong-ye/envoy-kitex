#include <algorithm>
#include <cerrno>
#include <cstring>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/ub_socket_handle_impl.h"

#include "test/mocks/api/mocks.h"
#include "test/test_common/threadsafe_singleton_injector.h"

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::_;
using testing::Invoke;
using testing::NiceMock;

namespace Envoy {
namespace Network {
namespace Ubsocket {

struct Iobuf {
  std::shared_ptr<std::vector<uint8_t>> storage;
  size_t offset;
  size_t size;
};

struct IobufChain {
  std::deque<Iobuf*> iobufs;
};

} // namespace Ubsocket
} // namespace Network
} // namespace Envoy

extern "C" void* ubsocket_iobuf_data(Envoy::Network::Ubsocket::Iobuf* iobuf);
extern "C" size_t ubsocket_iobuf_size(const Envoy::Network::Ubsocket::Iobuf* iobuf);
extern "C" void ubsocket_iobuf_release(Envoy::Network::Ubsocket::Iobuf* iobuf);

namespace Envoy {
namespace Network {
namespace {

struct FakeUbsocketState {
  Ubsocket::RxKind rx_kind{Ubsocket::RxKind::Eof};
  std::vector<std::string> rx_chunks;
  std::vector<std::weak_ptr<std::vector<uint8_t>>> allocations;
  std::string written;
  size_t payload_capacity{64 * 1024};
  bool fail_alloc{false};
  int alloc_calls{0};
  int copy_calls{0};
  int iobuf_releases{0};
  int chain_releases{0};
};

FakeUbsocketState* fake_ubsocket_state;

Ubsocket::Iobuf* makeIobuf(absl::string_view contents) {
  auto storage = std::make_shared<std::vector<uint8_t>>(contents.begin(), contents.end());
  fake_ubsocket_state->allocations.push_back(storage);
  return new Ubsocket::Iobuf{std::move(storage), 0, contents.size()};
}

class UbSocketHandleImplTest : public testing::Test {
protected:
  void SetUp() override { fake_ubsocket_state = &state_; }
  void TearDown() override { fake_ubsocket_state = nullptr; }

  FakeUbsocketState state_;
};

TEST_F(UbSocketHandleImplTest, AddsReceivedIobufsAsFragmentsAndReleasesOnDrain) {
  state_.rx_kind = Ubsocket::RxKind::Chain;
  state_.rx_chunks = {"hello", " world"};
  Buffer::OwnedImpl buffer;
  UbSocketHandleImpl handle;

  Api::IoCallUint64Result result = handle.read(buffer, 32);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(11, result.return_value_);
  EXPECT_EQ("hello world", buffer.toString());
  EXPECT_EQ(1, state_.chain_releases);
  EXPECT_EQ(0, state_.iobuf_releases);

  buffer.drain(buffer.length());
  EXPECT_EQ(2, state_.iobuf_releases);
}

TEST_F(UbSocketHandleImplTest, FallsBackToPosixReadWithoutConsumingTheStream) {
  state_.rx_kind = Ubsocket::RxKind::FallbackRequired;
  NiceMock<Api::MockOsSysCalls> os_sys_calls;
  TestThreadsafeSingletonInjector<Api::OsSysCallsImpl> os_calls(&os_sys_calls);
  EXPECT_CALL(os_sys_calls, recv(INVALID_SOCKET, _, _, 0))
      .WillOnce(Invoke([](os_fd_t, void* data, size_t length, int) -> Api::SysCallSizeResult {
        constexpr absl::string_view Contents = "copy";
        EXPECT_GE(length, Contents.size());
        std::memcpy(data, Contents.data(), Contents.size()); // NOLINT(safe-memcpy)
        return {static_cast<ssize_t>(Contents.size()), 0};
      }));
  Buffer::OwnedImpl buffer;
  UbSocketHandleImpl handle;

  Api::IoCallUint64Result result = handle.read(buffer, 32);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(4, result.return_value_);
  EXPECT_EQ("copy", buffer.toString());
}

TEST_F(UbSocketHandleImplTest, ReturnsZeroForEof) {
  state_.rx_kind = Ubsocket::RxKind::Eof;
  Buffer::OwnedImpl buffer;
  UbSocketHandleImpl handle;

  Api::IoCallUint64Result result = handle.read(buffer, 32);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(0, result.return_value_);
  EXPECT_TRUE(buffer.toString().empty());
}

TEST_F(UbSocketHandleImplTest, SendsManagedMemoryDirectly) {
  Ubsocket::Iobuf* managed = makeIobuf("managed");
  Buffer::RawSlice slice{ubsocket_iobuf_data(managed), ubsocket_iobuf_size(managed)};
  UbSocketHandleImpl handle;

  Api::IoCallUint64Result result = handle.writev(&slice, 1);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(7, result.return_value_);
  EXPECT_EQ("managed", state_.written);
  EXPECT_EQ(0, state_.alloc_calls);
  EXPECT_EQ(1, state_.iobuf_releases);
  ubsocket_iobuf_release(managed);
}

TEST_F(UbSocketHandleImplTest, StagesHeapMemoryInPayloadSizedIobufs) {
  state_.payload_capacity = 4;
  std::string heap = "123456789";
  Buffer::RawSlice slice{heap.data(), heap.size()};
  UbSocketHandleImpl handle;

  Api::IoCallUint64Result result = handle.writev(&slice, 1);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(heap.size(), result.return_value_);
  EXPECT_EQ(heap, state_.written);
  EXPECT_EQ(3, state_.alloc_calls);
  EXPECT_EQ(3, state_.iobuf_releases);
}

TEST_F(UbSocketHandleImplTest, FallsBackToWholeCopyWriteWhenStagingAllocationFails) {
  state_.fail_alloc = true;
  std::string heap = "copy fallback";
  Buffer::RawSlice slice{heap.data(), heap.size()};
  UbSocketHandleImpl handle;

  Api::IoCallUint64Result result = handle.writev(&slice, 1);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(heap.size(), result.return_value_);
  EXPECT_EQ(heap, state_.written);
  EXPECT_EQ(1, state_.alloc_calls);
  EXPECT_EQ(1, state_.copy_calls);
  EXPECT_EQ(0, state_.iobuf_releases);
}

} // namespace
} // namespace Network
} // namespace Envoy

extern "C" int ubsocket_recv_zc(int, size_t, Envoy::Network::Ubsocket::RxResult* result) {
  using Envoy::Network::Ubsocket::IobufChain;
  using Envoy::Network::Ubsocket::RxKind;
  auto& state = *Envoy::Network::fake_ubsocket_state;
  result->kind = state.rx_kind;
  result->size = 0;
  result->chain = nullptr;
  if (state.rx_kind != RxKind::Chain) {
    return 0;
  }

  result->chain = new IobufChain;
  for (const std::string& chunk : state.rx_chunks) {
    result->chain->iobufs.push_back(Envoy::Network::makeIobuf(chunk));
    result->size += chunk.size();
  }
  return 0;
}

extern "C" int ubsocket_iobuf_alloc(size_t capacity, Envoy::Network::Ubsocket::Iobuf** iobuf) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  ++state.alloc_calls;
  if (state.fail_alloc) {
    errno = ENOMEM;
    return -1;
  }
  auto storage = std::make_shared<std::vector<uint8_t>>(capacity);
  state.allocations.push_back(storage);
  *iobuf = new Envoy::Network::Ubsocket::Iobuf{std::move(storage), 0, capacity};
  return 0;
}

extern "C" int ubsocket_iobuf_chain_pop_front(Envoy::Network::Ubsocket::IobufChain* chain,
                                              Envoy::Network::Ubsocket::Iobuf** iobuf) {
  if (chain->iobufs.empty()) {
    *iobuf = nullptr;
    return 0;
  }
  *iobuf = chain->iobufs.front();
  chain->iobufs.pop_front();
  return 1;
}

extern "C" void* ubsocket_iobuf_data(Envoy::Network::Ubsocket::Iobuf* iobuf) {
  return iobuf->storage->data() + iobuf->offset;
}

extern "C" size_t ubsocket_iobuf_size(const Envoy::Network::Ubsocket::Iobuf* iobuf) {
  return iobuf->size;
}

extern "C" void ubsocket_iobuf_release(Envoy::Network::Ubsocket::Iobuf* iobuf) {
  ++Envoy::Network::fake_ubsocket_state->iobuf_releases;
  delete iobuf;
}

extern "C" void ubsocket_iobuf_chain_release(Envoy::Network::Ubsocket::IobufChain* chain) {
  for (Envoy::Network::Ubsocket::Iobuf* iobuf : chain->iobufs) {
    ubsocket_iobuf_release(iobuf);
  }
  ++Envoy::Network::fake_ubsocket_state->chain_releases;
  delete chain;
}

extern "C" int ubsocket_iobuf_query(const void* data, size_t length,
                                    Envoy::Network::Ubsocket::Iobuf** iobuf, size_t* offset) {
  auto& state = *Envoy::Network::fake_ubsocket_state;
  const auto* begin = static_cast<const uint8_t*>(data);
  for (const auto& weak_storage : state.allocations) {
    std::shared_ptr<std::vector<uint8_t>> storage = weak_storage.lock();
    if (storage == nullptr || begin < storage->data() ||
        begin + length > storage->data() + storage->size()) {
      continue;
    }
    *offset = static_cast<size_t>(begin - storage->data());
    *iobuf = new Envoy::Network::Ubsocket::Iobuf{std::move(storage), *offset, length};
    return 0;
  }
  errno = ENOENT;
  return -1;
}

extern "C" size_t ubsocket_iobuf_payload_capacity() {
  return Envoy::Network::fake_ubsocket_state->payload_capacity;
}

extern "C" ssize_t ubsocket_writev(int, const iovec* iov, int count) {
  auto& written = Envoy::Network::fake_ubsocket_state->written;
  ssize_t size = 0;
  for (int i = 0; i < count; ++i) {
    written.append(static_cast<const char*>(iov[i].iov_base), iov[i].iov_len);
    size += static_cast<ssize_t>(iov[i].iov_len);
  }
  return size;
}

extern "C" ssize_t ubsocket_writev_copy(int fd, const iovec* iov, int count) {
  ++Envoy::Network::fake_ubsocket_state->copy_calls;
  return ubsocket_writev(fd, iov, count);
}
