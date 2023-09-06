// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_
#define COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_

#include "commands/LinearHyperparams.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/define_named_tuple.hpp"

namespace commands {

using f_linear_regression =
    rfl::Field<"type_", rfl::Literal<"LinearRegression">>;

using LinearRegressionHyperparams = LinearHyperparams<
    rfl::define_named_tuple_t<LinearNamedTupleBase, f_linear_regression>>;

}  // namespace commands

#endif  // COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_
