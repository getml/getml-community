// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/PredictorParser.hpp"

#include "predictors/LinearRegression.hpp"
#include "predictors/LogisticRegression.hpp"
#include "predictors/XGBoostPredictor.hpp"

#include <rfl/visit.hpp>

namespace predictors {

rfl::Ref<Predictor> PredictorParser::parse(
    const PredictorHyperparams& _hyperparams,
    const rfl::Ref<const PredictorImpl>& _impl,
    const std::vector<commands::Fingerprint>& _dependencies) {
  const auto handle = [&_impl, &_dependencies](
                          const auto& _hyperparams) -> rfl::Ref<Predictor> {
    using Type = std::decay_t<decltype(_hyperparams)>;

    if constexpr (std::is_same<Type, LinearRegressionHyperparams>()) {
      return rfl::Ref<LinearRegression>::make(_hyperparams, _impl,
                                              _dependencies);
    }

    if constexpr (std::is_same<Type, LogisticRegressionHyperparams>()) {
      return rfl::Ref<LogisticRegression>::make(_hyperparams, _impl,
                                                _dependencies);
    }

    if constexpr (std::is_same<Type, XGBoostHyperparams>()) {
      return rfl::Ref<XGBoostPredictor>::make(_hyperparams, _impl,
                                              _dependencies);
    }
  };

  return rfl::visit(handle, _hyperparams);
}

}  // namespace predictors
