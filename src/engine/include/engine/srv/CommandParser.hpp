// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_SRV_COMMANDPARSER_HPP_
#define ENGINE_SRV_COMMANDPARSER_HPP_

#include "commands/DataFrameCommand.hpp"
#include "commands/DatabaseCommand.hpp"
#include "commands/PipelineCommand.hpp"
#include "commands/ProjectCommand.hpp"
#include "fct/define_tagged_union.hpp"

namespace engine {
namespace srv {

/// A RequestHandler handles all request in deploy mode.
struct CommandParser {
  using Command = fct::define_tagged_union_t<
      "type_", typename commands::DatabaseCommand::NamedTupleType,
      typename commands::DataFrameCommand::NamedTupleType,
      typename commands::PipelineCommand::NamedTupleType,
      typename commands::ProjectCommand::NamedTupleType>;

  /// Receives the command and parses it.
  /// Note that this function is so expensive in terms of compilation, that we
  /// outsource it into a separate file.
  static Command parse_cmd(const std::string& _cmd);
};

}  // namespace srv
}  // namespace engine

#endif  // ENGINE_SRV_REQUESTHANDLER_HPP_
