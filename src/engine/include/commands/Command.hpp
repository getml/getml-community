// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_COMMAND_HPP_
#define COMMANDS_COMMAND_HPP_

#include <variant>

#include "commands/CheckPipeline.hpp"

namespace commands {

using Command = std::variant<CheckPipeline>;

}  // namespace commands

#endif  // COMMANDS_COMMAND_HPP_
