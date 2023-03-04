// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/RequestHandler.hpp"
#include "json/json.hpp"

namespace engine {
namespace srv {

/// This function is so expensive in terms of compilation, that we outsource it
/// into a separate file.
typename RequestHandler::Command RequestHandler::recv_cmd() {
  const auto cmd = communication::Receiver::recv_cmd(logger_, &socket());
  return json::from_json<Command>(cmd);
}

}  // namespace srv
}  // namespace engine
