// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LOGISTICREGRESSIONHYPERPARMAS_HPP_
#define COMMANDS_LOGISTICREGRESSIONHYPERPARMAS_HPP_

#include "commands/LinearHyperparams.hpp"

#include <rfl/Literal.hpp>

namespace commands {

using LogisticRegressionHyperparams =
    LinearHyperparams<rfl::Literal<"LogisticRegression">>;

}  // namespace commands

#endif  // COMMANDS_LOGISTICREGRESSIONHYPERPARMAS_HPP_
