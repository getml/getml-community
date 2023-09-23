// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_
#define COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_

#include "commands/LinearHyperparams.hpp"
#include "rfl/Literal.hpp"

namespace commands {

using LinearRegressionHyperparams =
    LinearHyperparams<rfl::Literal<"LinearRegression">>;

}  // namespace commands

#endif  // COMMANDS_LINEARREGRESSIONHYPERPARMAS_HPP_
