// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMUNICATION_RECEIVER_HPP_
#define COMMUNICATION_RECEIVER_HPP_

#include <Poco/Net/StreamSocket.h>

#include <algorithm>
#include <rfl/Ref.hpp>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "communication/Logger.hpp"
#include "communication/ULong.hpp"
#include "helpers/Endianness.hpp"

namespace communication {

struct Receiver {
  static constexpr const char *GETML_SEP = "$GETML_SEP";

  /// Receives data of any type from the client
  template <class T>
  static void recv(const ULong _size, Poco::Net::StreamSocket *_socket,
                   T *_data);

  /// Receives a string from the client
  static std::string recv_string(Poco::Net::StreamSocket *_socket);

  /// Receives a command from the client
  static std::string recv_cmd(
      const rfl::Ref<const communication::Logger> &_logger,
      Poco::Net::StreamSocket *_socket);
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class T>
void Receiver::recv(const ULong _size, Poco::Net::StreamSocket *_socket,
                    T *_data) {
  const ULong len = 4096;

  ULong j = 0;

  // -------------------------------------------------------------------
  // Receive len bytes at most, write them into the buffer and then
  // copy to the output data.

  // This assumes that T* has enough data allocated.

  // We also assume that the size of the data to be sent is known.

  std::vector<char> buf(len);

  while (true) {
    const ULong current_len = std::min(len, _size - j);

    if (current_len == 0) {
      break;
    }

    if (current_len != len) {
      buf.resize(current_len);
    }

    for (size_t num_bytes_received = 0; num_bytes_received < buf.size();) {
      const auto nbytes = _socket->receiveBytes(
          buf.data() + num_bytes_received,
          static_cast<int>(buf.size() - num_bytes_received));

      if (nbytes <= 0) {
        throw std::runtime_error(
            "Broken pipe while attempting to receive "
            "data.");
      }

      num_bytes_received += static_cast<size_t>(nbytes);
    }

    for (size_t i = 0; i < buf.size(); ++i, ++j) {
      reinterpret_cast<char *>(_data)[j] = buf.data()[i];
    }
  }

  assert_true(j == _size);

  // -------------------------------------------------------------------
  // Handle endianness issues, which only apply for numeric types.
  // The only non-numeric type we ever come across is char.
  // By default, numeric data sent over the socket is big endian
  // (also referred to as network-byte-order)!

  // is_arithmetic includes numeric values and char.
  // http://en.cppreference.com/w/cpp/types/is_arithmetic
  static_assert(std::is_arithmetic<T>::value,
                "Only arithmetic types allowed for recv<T>(...)!");

  if (!std::is_same<T, char>::value &&
      helpers::Endianness::is_little_endian()) {
    std::for_each(
        _data, _data + static_cast<size_t>(_size) / sizeof(T),
        [](T &_val) { helpers::Endianness::reverse_byte_order(&_val); });
  }

  // -------------------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication

#endif  // COMMUNICATION_RECEIVER_HPP_
