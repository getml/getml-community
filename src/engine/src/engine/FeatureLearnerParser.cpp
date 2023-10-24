// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/FeatureLearnerParser.hpp"

#include <type_traits>

#include "fastprop/Hyperparameters.hpp"
#include "fastprop/algorithm/FastProp.hpp"
#include "fct/visit.hpp"
#include "featurelearners/FeatureLearner.hpp"

namespace engine {
namespace pipelines {

fct::Ref<featurelearners::AbstractFeatureLearner> FeatureLearnerParser::parse(
    const featurelearners::FeatureLearnerParams& _params,
    const commands::FeatureLearner& _hyperparameters) {
  const auto handle = [&_params](const auto& _hyperparameters)
      -> fct::Ref<featurelearners::AbstractFeatureLearner> {
    using Type = std::decay_t<decltype(_hyperparameters)>;

    if constexpr (std::is_same<Type, fastprop::Hyperparameters>()) {
      return fct::Ref<featurelearners::FeatureLearner<
          fastprop::algorithm::FastProp>>::make(_params, _hyperparameters);
    } else {
      throw std::runtime_error(
          "The " + fct::get<"type_">(_hyperparameters).name() +
          " feature learner is not supported in the community edition. Please "
          "upgrade to getML enterprise to use this. An overview of what is "
          "supported in the community edition can be found in the official "
          "getML documentation.");
    }
  };

  return fct::visit(handle, _hyperparameters);
}

// ----------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
