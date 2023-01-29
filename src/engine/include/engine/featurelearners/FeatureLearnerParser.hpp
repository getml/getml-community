// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_
#define ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_

#include "engine/featurelearners/AbstractFeatureLearner.hpp"
#include "engine/featurelearners/FeatureLearnerParams.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace featurelearners {

struct FeatureLearnerParser {
  /// Returns the correct feature learner
  static fct::Ref<AbstractFeatureLearner> parse(
      const FeatureLearnerParams& _params);
};

}  // namespace featurelearners
}  // namespace engine

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_

