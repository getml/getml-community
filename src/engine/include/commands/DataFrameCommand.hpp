// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATAFRAMECOMMAND_HPP_
#define COMMANDS_DATAFRAMECOMMAND_HPP_

#include <string>

#include "commands/Int.hpp"
#include "commands/Pipeline.hpp"
#include "commands/ProjectCommand.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"

namespace commands {

/// Any command to be handled by the DataFrameManager.
struct DataFrameCommand {
  /// The command to add a data frame from arrow.
  using AddDfFromArrowOp = typename ProjectCommand::AddDfFromArrowOp;

  /// The command to add a data frame from CSV.
  using AddDfFromCSVOp = typename ProjectCommand::AddDfFromCSVOp;

  /// The command to add a data frame from a database.
  using AddDfFromDBOp = typename ProjectCommand::AddDfFromDBOp;

  /// The command to add a data frame from JSON.
  using AddDfFromJSONOp = typename ProjectCommand::AddDfFromJSONOp;

  /// The command to add a data frame from parquet.
  using AddDfFromParquetOp = typename ProjectCommand::AddDfFromParquetOp;

  /// The command to add a data frame from JSON.
  using AddDfFromQueryOp = typename ProjectCommand::AddDfFromQueryOp;

  /// The command to add a data frame from a View.
  using AddDfFromViewOp = typename ProjectCommand::AddDfFromViewOp;

  using NamedTupleType =
      fct::TaggedUnion<"type_", AddDfFromArrowOp, AddDfFromCSVOp, AddDfFromDBOp,
                       AddDfFromJSONOp, AddDfFromParquetOp, AddDfFromQueryOp,
                       AddDfFromViewOp>;

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATAFRAMECOMMAND_HPP_
