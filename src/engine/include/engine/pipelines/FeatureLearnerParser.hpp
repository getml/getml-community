// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FEATURELEARNERPARSER_HPP_
#define ENGINE_PIPELINES_FEATURELEARNERPARSER_HPP_

#include "commands/FeatureLearner.hpp"
#include "fct/Ref.hpp"
#include "featurelearners/AbstractFeatureLearner.hpp"
#include "featurelearners/FeatureLearnerParams.hpp"

namespace engine {
namespace pipelines {

struct FeatureLearnerParser {
  /// Returns the correct feature learner
  static fct::Ref<featurelearners::AbstractFeatureLearner> parse(
      const featurelearners::FeatureLearnerParams& _params,
      const commands::FeatureLearner& _hyperparameters);
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FEATURELEARNERPARSER_HPP_

