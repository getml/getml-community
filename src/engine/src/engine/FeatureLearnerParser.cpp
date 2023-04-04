// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/featurelearners/FeatureLearnerParser.hpp"

#include <type_traits>

#include "engine/featurelearners/FeatureLearner.hpp"
#include "fastprop/Hyperparameters.hpp"
#include "fastprop/algorithm/algorithm.hpp"
#include "fct/visit.hpp"

namespace engine {
namespace featurelearners {

fct::Ref<AbstractFeatureLearner> FeatureLearnerParser::parse(
    const FeatureLearnerParams& _params,
    const commands::FeatureLearner& _hyperparameters) {
  const auto handle =
      [&_params](
          const auto& _hyperparameters) -> fct::Ref<AbstractFeatureLearner> {
    using Type = std::decay_t<decltype(_hyperparameters)>;

    if constexpr (std::is_same<Type, fastprop::Hyperparameters>()) {
      return fct::Ref<FeatureLearner<fastprop::algorithm::FastProp>>::make(
          _params, _hyperparameters);
    }
  };

  return fct::visit(handle, _hyperparameters);
}

// ----------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine
