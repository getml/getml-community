// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_ARROWSOCKETINPUTSTREAM_HPP_
#define ENGINE_HANDLERS_ARROWSOCKETINPUTSTREAM_HPP_

#include "communication/Receiver.hpp"
#include "engine/ULong.hpp"

#include <Poco/Net/StreamSocket.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>

#include <cstdint>
#include <stdexcept>

namespace engine {
namespace handlers {

class ArrowSocketInputStream final : public arrow::io::InputStream {
 public:
  explicit ArrowSocketInputStream(Poco::Net::StreamSocket *_socket);

  ~ArrowSocketInputStream() final = default;

 public:
  /// Close the stream cleanly.
  arrow::Status Close() final {
    closed_ = true;
    return arrow::Status::OK();
  }

  /// Return whether the stream is closed.
  bool closed() const final { return closed_; }

  arrow::Result<std::int64_t> Read(std::int64_t _nbytes, void *_out) final {
    position_ += _nbytes;
    communication::Receiver::recv<char>(static_cast<ULong>(_nbytes), socket_,
                                        reinterpret_cast<char *>(_out));
    return arrow::Result<std::int64_t>(_nbytes);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(
      std::int64_t _nbytes) final {
    position_ += _nbytes;

    auto data = std::vector<char>(_nbytes);

    communication::Receiver::recv<char>(static_cast<ULong>(_nbytes), socket_,
                                        data.data());

    arrow::BufferBuilder builder;

    auto status = builder.Resize(_nbytes);

    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }

    status = builder.Append(data.data(), _nbytes);

    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }

    std::shared_ptr<arrow::Buffer> buffer;

    status = builder.Finish(&buffer);

    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }

    return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer);
  }

  /// Return the position in this stream.
  arrow::Result<std::int64_t> Tell() const final { return position_; }

 private:
  /// Whether the stream has been closed
  bool closed_;

  /// The current position.
  std::int64_t position_;

  /// A pointer to the underlying socket
  Poco::Net::StreamSocket *socket_;
};

// -------------------------------------------------------------------------

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_ARROWSOCKETINPUTSTREAM_HPP_
