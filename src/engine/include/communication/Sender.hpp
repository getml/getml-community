// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMUNICATION_SENDER_HPP_
#define COMMUNICATION_SENDER_HPP_

#include "communication/Float.hpp"
#include "communication/Receiver.hpp"
#include "communication/ULong.hpp"
#include "containers/Column.hpp"
#include "containers/NumericalFeatures.hpp"
#include "helpers/Endianness.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace communication {

struct Sender {
  static constexpr const char* GETML_SEP = Receiver::GETML_SEP;
  static constexpr std::uint64_t SEP_SIZE = 10;

  /// Sends data of any kind to the client
  template <class T>
  static void send(const ULong _size, const T* _data,
                   Poco::Net::StreamSocket* _socket);

  /// Sends a categorical column to the client
  static void send_categorical_column(const std::vector<std::string>& _col,
                                      Poco::Net::StreamSocket* _socket);

  /// Sends features to the client, transposing them.
  static void send_features(const containers::NumericalFeatures& _features,
                            Poco::Net::StreamSocket* _socket);

  /// Sends a vector to the client
  static void send_column(const containers::Column<Float>& _col,
                          Poco::Net::StreamSocket* _socket);

  /// Sends a string to the client
  static void send_string(const std::string& _string,
                          Poco::Net::StreamSocket* _socket);
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class T>
void Sender::send(const ULong _size, const T* _data,
                  Poco::Net::StreamSocket* _socket) {
  const ULong len = 4096;

  const bool is_little_endian = helpers::Endianness::is_little_endian();

  ULong j = 0;

  std::vector<char> buf(len);

  // -----------------------------------------------------------------------
  // Send len bytes at most into the buffer and then send to socket.

  while (true) {
    // ---------------------------------------------------------------
    // Copy to buffer

    const ULong current_len = std::min(len, _size - j);

    if (current_len == 0) {
      break;
    }

    if (current_len != len) {
      buf.resize(current_len);
    }

    for (ULong i = 0; i < current_len; ++i, ++j) {
      buf.data()[i] = reinterpret_cast<const char*>(_data)[j];
    }

    // ---------------------------------------------------------------
    // Handle endianness issues, which only apply for numeric types.
    // The only non-numeric type we ever come across is char.
    // By default, numeric data sent over the socket is big endian
    // (also referred to as network-byte-order)!

    // is_arithmetic includes numeric values and char.
    // http://en.cppreference.com/w/cpp/types/is_arithmetic
    static_assert(std::is_arithmetic<T>::value,
                  "Only arithmetic types allowed for Sender::send<T>(...)!");

    if (!std::is_same<T, char>::value && is_little_endian) {
      T* buf_begin = reinterpret_cast<T*>(buf.data());

      T* buf_end = buf_begin + buf.size() / sizeof(T);

      std::for_each(buf_begin, buf_end, [](T& _val) {
        helpers::Endianness::reverse_byte_order(&_val);
      });
    }

    // ---------------------------------------------------------------
    // Send via socket

    for (size_t num_bytes_sent = 0; num_bytes_sent < buf.size();) {
      const auto nbytes =
          _socket->sendBytes(buf.data() + num_bytes_sent,
                             static_cast<int>(buf.size() - num_bytes_sent));

      if (nbytes <= 0) {
        throw std::runtime_error("Broken pipe while attempting to send data.");
      }

      num_bytes_sent += static_cast<size_t>(nbytes);
    }

    // -------------------------------------------------
  }

  assert_true(j == _size);
}

// ------------------------------------------------------------------------
}  // namespace communication

#endif  // COMMUNICATION_SENDER_HPP_
