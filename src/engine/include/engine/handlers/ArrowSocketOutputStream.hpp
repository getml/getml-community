// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_ARROWSOCKETOUTPUTSTREAM_HPP_
#define ENGINE_HANDLERS_ARROWSOCKETOUTPUTSTREAM_HPP_

#include <Poco/Net/StreamSocket.h>
#include <arrow/api.h>
#include <arrow/ipc/api.h>

#include <cstdint>
#include <stdexcept>

#include "engine/communication/communication.hpp"

namespace engine {
namespace handlers {

class ArrowSocketOutputStream : public arrow::io::OutputStream {
 public:
  ArrowSocketOutputStream(Poco::Net::StreamSocket *_socket)
      : closed_(false), position_(0), socket_(_socket) {}

  virtual ~ArrowSocketOutputStream() {}

 public:
  /// Close the stream cleanly.
  arrow::Status Close() final {
    closed_ = true;
    return arrow::Status::OK();
  }

  /// Return whether the stream is closed.
  bool closed() const final { return closed_; }

  /// Write the given data to the stream.
  /// This method always processes the bytes in full. Depending on the
  /// semantics of the stream, the data may be written out immediately, held
  /// in a buffer, or written asynchronously. In the case where the stream
  /// buffers the data, it will be copied. To avoid potentially large copies,
  /// use the Write variant that takes an owned Buffer.
  arrow::Status Write(const void *_data, std::int64_t _nbytes) final {
    position_ += _nbytes;
    communication::Sender::send<char>(static_cast<ULong>(_nbytes),
                                      reinterpret_cast<const char *>(_data),
                                      socket_);
    return arrow::Status::OK();
  }

  /// Write the given data to the stream.
  /// Since the Buffer owns its memory, this method can avoid a copy if
  /// buffering is required. See Write(const void*, int64_t) for details.
  arrow::Status Write(const std::shared_ptr<arrow::Buffer> &_data) final {
    assert_true(_data);
    position_ += _data->size();
    communication::Sender::send<char>(
        static_cast<ULong>(_data->size()),
        reinterpret_cast<const char *>(_data->data()), socket_);
    return arrow::Status::OK();
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

#endif  // ENGINE_HANDLERS_ARROWSOCKETOUTPUTSTREAM_HPP_
