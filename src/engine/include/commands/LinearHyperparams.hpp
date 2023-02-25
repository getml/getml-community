// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_LINEARHYPERPARMS_HPP_
#define COMMANDS_LINEARHYPERPARMS_HPP_

#include "commands/Float.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace commands {

using LinearNamedTupleBase =
    fct::NamedTuple<fct::Field<"learning_rate_", Float>,
                    fct::Field<"reg_lambda_", Float>>;

/// Hyperparameters for Linear models.
template <class T>
struct LinearHyperparams {
  using NamedTupleType = T;

  LinearHyperparams(const Float &_reg_lambda, const Float &_learning_rate)
      : val_(fct::make_field<"learning_rate_">(_learning_rate) *
             fct::make_field<"reg_lambda_">(_reg_lambda)) {}

  LinearHyperparams(const NamedTupleType &_val) : val_(_val) {}

  ~LinearHyperparams() = default;

  /// Trivial accessor
  Float learning_rate() const { return fct::get<"learning_rate_">(val_); }

  /// Trivial accessor
  Float reg_lambda() const { return fct::get<"reg_lambda_">(val_); }

  /// The underlying named tuple
  const NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_LINEARHYPERPARMS_HPP_
