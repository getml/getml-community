// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_PARAMS_HPP_
#define ENGINE_PREPROCESSORS_PARAMS_HPP_

#include <memory>
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/SocketLogger.hpp"
#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "helpers/Placeholder.hpp"

namespace engine {
namespace preprocessors {

struct Params {
  /// The Encoding used for the categories.
  rfl::Field<"categories_", rfl::Ref<containers::Encoding>> categories;

  /// Contains all of the names of all data frames or views needed for fitting
  /// the pipeline.
  rfl::Field<"cmd_", commands::DataFramesOrViews> cmd;

  /// Logs the progress.
  rfl::Field<"logger_", std::shared_ptr<const communication::SocketLogger>>
      logger;

  /// The percentage at which we want the logging to begin.
  rfl::Field<"logging_begin_", size_t> logging_begin;

  /// The percentage at which we want the logging to end.
  rfl::Field<"logging_end_", size_t> logging_end;

  /// The peripheral tables.
  rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>
      peripheral_dfs;

  /// The peripheral tables.
  rfl::Field<"peripheral_names_", std::vector<std::string>> peripheral_names;

  /// The peripheral tables.
  rfl::Field<"placeholder_", helpers::Placeholder> placeholder;

  /// The population table.
  rfl::Field<"population_df_", containers::DataFrame> population_df;
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PARAMS_HPP_
