// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_TRANSFORMPARAMS_HPP_
#define FEATURELEARNERS_TRANSFORMPARAMS_HPP_

#include <cstddef>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "featurelearners/FitParams.hpp"
#include "rfl/Field.hpp"
#include "rfl/Flatten.hpp"
#include "rfl/NamedTuple.hpp"

namespace featurelearners {

struct TransformParams {
  /// Contains all of the names of all data frames or views needed for fitting
  /// the pipeline.
  rfl::Field<"cmd_", commands::DataFramesOrViews> cmd;

  /// Indicates which features we want to generate.
  rfl::Field<"index_", std::vector<size_t>> index;

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

#endif  // FEATURELEARNERS_TRANSFORMPARAMS_HPP_

