#ifndef ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_
#define ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_

// ----------------------------------------------------------------------------

#include "fct/Ref.hpp"

// ----------------------------------------------------------------------------

#include "engine/featurelearners/AbstractFeatureLearner.hpp"
#include "engine/featurelearners/FeatureLearnerParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace featurelearners {

struct FeatureLearnerParser {
  /// Returns the correct feature learner
  static fct::Ref<AbstractFeatureLearner> parse(
      const FeatureLearnerParams& _params);
};

// ----------------------------------------------------------------------------

}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_

