// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_GETBOOLEANCOLUMNNROWS_HPP_
#define ENGINE_COMMANDS_GETBOOLEANCOLUMNNROWS_HPP_

#include "engine/commands/GetBooleanColumn.hpp"
#include "engine/commands/GetBooleanColumnNRows.hpp"

namespace engine {
namespace commands {

/// Defines the command necessary tp retrieve a boolean column.
using GetBooleanColumnNRows = GetBooleanColumn;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_GETBOOLEANCOLUMNNROWS_HPP_

