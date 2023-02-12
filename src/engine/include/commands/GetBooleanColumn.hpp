// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_GETBOOLEANCOLUMN_HPP_
#define COMMANDS_GETBOOLEANCOLUMN_HPP_

#include "commands/BasicCommand.hpp"
#include "commands/BooleanColumnView.hpp"
#include "fct/Field.hpp"
#include "fct/make_named_tuple.hpp"

namespace commands {

/// Defines the command necessary tp retrieve a boolean column.
using GetBooleanColumn =
    fct::make_named_tuple<BasicCommand, fct::Field<"col_", BooleanColumnView>>;

}  // namespace commands

#endif  // COMMANDS_GETBOOLEANCOLUMN_HPP_

