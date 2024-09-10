// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_FITPARAMS_HPP_
#define FEATURELEARNERS_FITPARAMS_HPP_

#include <memory>
#include <optional>
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/SocketLogger.hpp"
#include "containers/DataFrame.hpp"

namespace featurelearners {

struct FitParams {
  /// Contains all of the names of all data frames or views needed for fitting
  /// the pipeline.
  rfl::Field<"cmd_", commands::DataFramesOrViews> cmd;

  /// The peripheral tables.
  rfl::Field<"peripheral_dfs_", std::vector<containers::DataFrame>>
      peripheral_dfs;

  /// The population table.
  rfl::Field<"population_df_", containers::DataFrame> population_df;

  /// The prefix, used to identify the feature learner.
  rfl::Field<"prefix_", std::string> prefix;

  /// Logs the progress.
  rfl::Field<"socket_logger_",
             std::shared_ptr<const communication::SocketLogger>>
      socket_logger;

  /// The prefix, used to identify the feature learner.
  rfl::Field<"temp_dir_", std::optional<std::string>> temp_dir;
};

}  // namespace featurelearners

#endif  // FEATURELEARNERS_FITPARAMS_HPP_
