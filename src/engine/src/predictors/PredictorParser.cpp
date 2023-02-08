// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/PredictorParser.hpp"

#include "fct/visit.hpp"
#include "predictors/LinearRegression.hpp"
#include "predictors/LogisticRegression.hpp"
#include "predictors/XGBoostPredictor.hpp"

namespace predictors {

fct::Ref<Predictor> PredictorParser::parse(
    const PredictorHyperparams& _hyperparams,
    const std::shared_ptr<const PredictorImpl>& _impl,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  const auto handle = [&_impl, &_dependencies](
                          const auto& _hyperparams) -> fct::Ref<Predictor> {
    using Type = std::decay_t<decltype(_hyperparams)>;

    if constexpr (std::is_same<Type, LinearRegressionHyperparams>()) {
      return fct::Ref<LinearRegression>::make(_hyperparams, _impl,
                                              _dependencies);
    }

    if constexpr (std::is_same<Type, LogisticRegressionHyperparams>()) {
      return fct::Ref<LogisticRegression>::make(_hyperparams, _impl,
                                                _dependencies);
    }

    if constexpr (std::is_same<Type, XGBoostHyperparams>()) {
      return fct::Ref<XGBoostPredictor>::make(_hyperparams, _impl,
                                              _dependencies);
    }
  };

  return fct::visit(handle, _hyperparams);
}

}  // namespace predictors
