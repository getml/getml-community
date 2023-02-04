// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_LINEARHYPERPARMS_HPP_
#define PREDICTORS_LINEARHYPERPARMS_HPP_

#include <Poco/JSON/Object.h>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "predictors/Float.hpp"
#include "predictors/JSON.hpp"

namespace predictors {

/// Learning rate used for the updates
using f_learning_rate = fct::Field<"learning_rate_", Float>;

/// L2 regularization term on weights
using f_reg_lambda = fct::Field<"reg_lambda_", Float>;

using LinearNamedTupleBase = fct::NamedTuple<f_learning_rate, f_reg_lambda>;

/// Hyperparameters for Linear models.
template <class T>
struct LinearHyperparams {
  using NamedTupleType = T;

  LinearHyperparams(const Float &_reg_lambda, const Float &_learning_rate)
      : val_(f_learning_rate(_learning_rate) * f_reg_lambda(_reg_lambda)) {}

  LinearHyperparams(const NamedTupleType &_val) : val_(_val) {}

  /// TODO: Remove this quick fix.
  LinearHyperparams(const Poco::JSON::Object &_json_obj)
      : val_(json::from_json<NamedTupleType>(_json_obj)) {}

  ~LinearHyperparams() = default;

  /// Trivial accessor
  Float learning_rate() const { return fct::get<f_learning_rate>(val_); }

  /// Trivial accessor
  Float reg_lambda() const { return fct::get<f_reg_lambda>(val_); }

  /// The underlying named tuple
  const NamedTupleType val_;
};

}  // namespace predictors

#endif  // PREDICTORS_LINEARHYPERPARMS_HPP_
