// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/CommandParser.hpp"

#include "json/json.hpp"

namespace engine {
namespace srv {

/// This function is so expensive in terms of compilation, that we outsource it
/// into a separate file.
typename CommandParser::Command CommandParser::parse_cmd(
    const std::string& _cmd) {
  return json::from_json<Command>(_cmd);
}

}  // namespace srv
}  // namespace engine
