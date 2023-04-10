// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_COMMAND_HPP_
#define COMMANDS_COMMAND_HPP_

#include <Poco/Dynamic/Var.h>

#include <variant>

#include "commands/ColumnCommand.hpp"
#include "commands/DataFrameCommand.hpp"
#include "commands/DatabaseCommand.hpp"
#include "commands/PipelineCommand.hpp"
#include "commands/ProjectCommand.hpp"
#include "commands/ViewCommand.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace commands {

struct Command {
  using IsAliveOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"is_alive">>>;

  using MonitorURLOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"monitor_url">>>;

  using ShutdownOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"shutdown">>>;

  using NamedTupleType =
      std::variant<ColumnCommand, DatabaseCommand, DataFrameCommand,
                   PipelineCommand, ProjectCommand, ViewCommand, IsAliveOp,
                   MonitorURLOp, ShutdownOp>;

  static Command from_json_obj(const Poco::Dynamic::Var& _obj);

  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_COMMAND_HPP_
