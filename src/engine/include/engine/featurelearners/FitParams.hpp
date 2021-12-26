#ifndef ENGINE_FEATURELEARNERS_FITPARAMS_HPP_
#define ENGINE_FEATURELEARNERS_FITPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "engine/Int.hpp"
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace featurelearners {
// ----------------------------------------------------------------------------

struct FitParams {
  /// The command used.
  const Poco::JSON::Object cmd_;

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

// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

#endif  // ENGINE_FEATURELEARNERS_FITPARAMS_HPP_

