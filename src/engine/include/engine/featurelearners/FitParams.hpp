// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_FEATURELEARNERS_FITPARAMS_HPP_
#define ENGINE_FEATURELEARNERS_FITPARAMS_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "engine/Int.hpp"
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"

namespace engine {
namespace featurelearners {

struct FitParams {
  /// The command used.
  const commands::DataFramesOrViews cmd_;

  /// Logs the progress.
  const std::shared_ptr<const communication::SocketLogger> logger_;

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
}  // namespace engine

#endif  // ENGINE_FEATURELEARNERS_FITPARAMS_HPP_

