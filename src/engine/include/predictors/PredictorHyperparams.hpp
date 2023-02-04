// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_PREDICTORHYPERPARAMS_HPP_
#define PREDICTORS_PREDICTORHYPERPARAMS_HPP_

#include <variant>

#include "fct/Ref.hpp"
#include "predictors/LinearRegressionHyperparams.hpp"
#include "predictors/LogisticRegressionHyperparams.hpp"
#include "predictors/XGBoostHyperparams.hpp"

namespace predictors {

using PredictorHyperparams =
    std::variant<fct::Ref<const LinearRegressionHyperparams>,
                 fct::Ref<const LogisticRegressionHyperparams>,
                 fct::Ref<const XGBoostHyperparams>>;

}  // namespace predictors

#endif  // ENGINE_PREDICTORS_PREDICTORHYPERPARAMS_HPP_
