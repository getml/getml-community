// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "communication/Receiver.hpp"

#include "communication/Int.hpp"

namespace communication {

std::string Receiver::recv_cmd(
    const rfl::Ref<const communication::Logger> &_logger,
    Poco::Net::StreamSocket *_socket) {
  const auto str = Receiver::recv_string(_socket);

  std::stringstream cmd_log;
  cmd_log << "Command sent by " << _socket->peerAddress().toString() << ":"
          << std::endl
          << str;
  _logger->log(cmd_log.str());

  return str;
}

// -----------------------------------------------------------------------------

std::string Receiver::recv_string(Poco::Net::StreamSocket *_socket) {
  std::vector<Int> str_length(1);

  Receiver::recv<Int>(sizeof(Int), _socket, str_length.data());

  std::string str(str_length[0], '0');

  const auto str_length_long = static_cast<ULong>(str_length[0]);

  Receiver::recv<char>(str_length_long,  // sizeof(char) = 1 by definition
                       _socket, &str[0]);

  return str;
}

// -----------------------------------------------------------------------------
}  // namespace communication
