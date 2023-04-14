// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_FITPARAMS_HPP_
#define FEATURELEARNERS_FITPARAMS_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "containers/containers.hpp"
#include "engine/communication/communication.hpp"
#include "featurelearners/Int.hpp"

namespace featurelearners {

struct FitParams {
  /// The command used.
  const commands::DataFramesOrViews cmd_;

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

#endif  // FEATURELEARNERS_FITPARAMS_HPP_

