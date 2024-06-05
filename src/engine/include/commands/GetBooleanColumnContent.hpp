// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_GETBOOLEANCOLUMNCONTENT_HPP_
#define COMMANDS_GETBOOLEANCOLUMNCONTENT_HPP_

#include "commands/BasicCommand.hpp"
#include "commands/BooleanColumnView.hpp"
#include "commands/GetContent.hpp"
#include <rfl/Field.hpp>
#include <rfl/make_named_tuple.hpp>

namespace commands {

/// Defines the command necessary to retrieve the content of
// a boolean column.
using GetBooleanColumnContent =
    rfl::make_named_tuple<BasicCommand, GetContent,
                          rfl::Field<"col_", BooleanColumnView>>;

}  // namespace commands

#endif  // COMMANDS_GETBOOLEANCOLUMN_HPP_

