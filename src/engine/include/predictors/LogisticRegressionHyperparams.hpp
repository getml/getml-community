// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_LOGISTICREGRESSIONHYPERPARMAS_HPP_
#define PREDICTORS_LOGISTICREGRESSIONHYPERPARMAS_HPP_

#include <Poco/JSON/Object.h>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/define_named_tuple.hpp"
#include "predictors/LinearHyperparams.hpp"

namespace predictors {

using f_logistic_regression =
    fct::Field<"type_", fct::Literal<"LogisticRegression">>;

using LogisticRegressionHyperparams = LinearHyperparams<
    fct::define_named_tuple_t<LinearNamedTupleBase, f_logistic_regression>>;

}  // namespace predictors

#endif  // PREDICTORS_LOGISTICREGRESSIONHYPERPARMAS_HPP_
