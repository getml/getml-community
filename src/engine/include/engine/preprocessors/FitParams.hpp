#ifndef ENGINE_PREPROCESSORS_FITPARAMS_HPP_
#define ENGINE_PREPROCESSORS_FITPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "fct/Ref.hpp"
#include "helpers/helpers.hpp"
#include "logging/logging.hpp"

// ----------------------------------------------------------------------------

#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace preprocessors {

struct FitParams {
  /// The command used to create the preprocessor.
  const Poco::JSON::Object cmd_;

  /// Encoding for categorical variables.
  const fct::Ref<containers::Encoding> categories_;

  /// The logger used to log the progress.
  const std::shared_ptr<const logging::AbstractLogger> logger_;

  /// The percentage at which we want the logging to begin.
  const size_t logging_begin_;

  /// The percentage at which we want the logging to end.
  const size_t logging_end_;

  /// The peripheral data frames.
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The names of the peripheral data frames as referred to in the SQL code.
  const std::vector<std::string> peripheral_names_;

  /// The placeholder for the data model.
  const helpers::Placeholder placeholder_;

  /// The population data frame.
  const containers::DataFrame population_df_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_FITPARAMS_HPP_
