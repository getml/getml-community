// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/ArrowSocketInputStream.hpp"

namespace engine::handlers {

ArrowSocketInputStream::ArrowSocketInputStream(Poco::Net::StreamSocket *_socket)
    : closed_(false), position_(0), socket_(_socket) {}

}  // namespace engine::handlers
