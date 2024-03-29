// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_COMMAND_HPP_
#define COMMANDS_COMMAND_HPP_

#include <variant>

#include "commands/ColumnCommand.hpp"
#include "commands/DataFrameCommand.hpp"
#include "commands/DatabaseCommand.hpp"
#include "commands/PipelineCommand.hpp"
#include "commands/ProjectCommand.hpp"
#include "commands/ViewCommand.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/json.hpp"

namespace commands {

struct Command {
  using IsAliveOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"is_alive">>>;

  using MonitorURLOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"monitor_url">>>;

  using ShutdownOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"shutdown">>>;

  using ReflectionType =
      std::variant<ColumnCommand, DatabaseCommand, DataFrameCommand,
                   PipelineCommand, ProjectCommand, ViewCommand, IsAliveOp,
                   MonitorURLOp, ShutdownOp>;

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static Command from_json_obj(const InputVarType& _obj);

  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_COMMAND_HPP_
