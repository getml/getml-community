#ifndef ENGINE_PIPELINES_PREPROCESSED_HPP_
#define ENGINE_PIPELINES_PREPROCESSED_HPP_

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "engine/containers/DataFrame.hpp"
#include "fct/fct.hpp"

// ----------------------------------------------------------------------------

#include "engine/preprocessors/preprocessors.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

struct Preprocessed {
  /// The fingerprints of the data frames used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> df_fingerprints_;

  /// The modified peripheral data frames (after applying the preprocessors)
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The modified population data frame (after applying the preprocessors)
  const containers::DataFrame population_df_;

  /// The preprocessors used in this pipeline.
  const std::vector<fct::Ref<const preprocessors::Preprocessor>> preprocessors_;

  /// The fingerprints of the preprocessor used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> preprocessor_fingerprints_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PREPROCESSED_HPP_

