// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_
#define COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_

#include "commands/LinearHyperparams.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/define_named_tuple.hpp"

namespace commands {

using f_linear_regression =
    fct::Field<"type_", fct::Literal<"LinearRegression">>;

using LinearRegressionHyperparams = LinearHyperparams<
    fct::define_named_tuple_t<LinearNamedTupleBase, f_linear_regression>>;

}  // namespace commands

#endif  // COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_
