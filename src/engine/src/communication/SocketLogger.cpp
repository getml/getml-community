// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "communication/SocketLogger.hpp"

namespace communication {

SocketLogger::SocketLogger(
    const std::shared_ptr<const communication::Logger>& _logger,
    const bool _silent, Poco::Net::StreamSocket* _socket)
    : logger_(_logger), silent_(_silent), socket_(_socket) {
  assert_true(logger_);
}

}  // namespace communication
