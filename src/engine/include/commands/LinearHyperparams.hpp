// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LINEARHYPERPARMS_HPP_
#define COMMANDS_LINEARHYPERPARMS_HPP_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

#include "commands/Float.hpp"

namespace commands {

/// Hyperparameters for Linear models.
template <class LiteralType>
struct LinearHyperparams {
  /// The underlying type.
  using Tag = LiteralType;

  /// The learning rate, for numerical optimization.
  rfl::Field<"learning_rate_", Float> learning_rate;

  /// The regularization factor.
  rfl::Field<"reg_lambda_", Float> reg_lambda;
};

}  // namespace commands

#endif  // COMMANDS_LINEARHYPERPARMS_HPP_
