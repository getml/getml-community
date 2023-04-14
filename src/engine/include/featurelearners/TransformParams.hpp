// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_TRANSFORMPARAMS_HPP_
#define FEATURELEARNERS_TRANSFORMPARAMS_HPP_

#include "commands/DataFramesOrViews.hpp"
#include "engine/communication/communication.hpp"
namespace featurelearners {

struct TransformParams {
  /// The command used.
  const commands::DataFramesOrViews cmd_;

  /// Indicates which features we want to generate.
  const std::vector<size_t> index_;

  /// Logs the progress.
  const std::shared_ptr<const engine::communication::SocketLogger> logger_;

  /// The peripheral data frames.
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The main data frame, defining the population.
  const containers::DataFrame population_df_;

  /// The prefix, used to identify the feature learner.
  const std::string prefix_;

  /// The temporary directory, used for the memory mappings
  const std::optional<std::string> temp_dir_;
};

}  // namespace featurelearners

#endif  // FEATURELEARNERS_TRANSFORMPARAMS_HPP_

