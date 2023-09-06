// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LOGISTICREGRESSIONHYPERPARMAS_HPP_
#define COMMANDS_LOGISTICREGRESSIONHYPERPARMAS_HPP_

#include "commands/LinearHyperparams.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/define_named_tuple.hpp"

namespace commands {

using f_logistic_regression =
    rfl::Field<"type_", rfl::Literal<"LogisticRegression">>;

using LogisticRegressionHyperparams = LinearHyperparams<
    rfl::define_named_tuple_t<LinearNamedTupleBase, f_logistic_regression>>;

}  // namespace commands

#endif  // COMMANDS_LOGISTICREGRESSIONHYPERPARMAS_HPP_
