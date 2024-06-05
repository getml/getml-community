// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_BASICCOMMAND_HPP_
#define COMMANDS_BASICCOMMAND_HPP_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

namespace commands {

/// Infers the type of the command.
using f_name = rfl::Field<"name_", std::string>;

/// The type of the command - so we can correctly identify this.
using f_type = rfl::Field<"type_", std::string>;

/// The basis for all other commands.
using BasicCommand = rfl::NamedTuple<f_name, f_type>;

}  // namespace commands

#endif  // COMMANDS_BASICCOMMAND_HPP_
