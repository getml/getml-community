#ifndef PREDICTORS_LINEARHYPERPARMS_HPP_
#define PREDICTORS_LINEARHYPERPARMS_HPP_

// ----------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------

#include "predictors/Float.hpp"
#include "predictors/JSON.hpp"

// ----------------------------------------------------------------------

namespace predictors {
// ----------------------------------------------------------------------

/// Hyperparameters for Linear models.
struct LinearHyperparams {
  // -----------------------------------------

  LinearHyperparams(const Float &_reg_lambda, const Float &_learning_rate)
      : learning_rate_(_learning_rate), reg_lambda_(_reg_lambda) {}

  LinearHyperparams(const Poco::JSON::Object &_json_obj)
      : learning_rate_(JSON::get_value<Float>(_json_obj, "learning_rate_")),
        reg_lambda_(JSON::get_value<Float>(_json_obj, "reg_lambda_")) {}

  ~LinearHyperparams() = default;

  // -----------------------------------------

  /// Learning rate used for the updates
  const Float learning_rate_;

  /// L2 regularization term on weights
  const Float reg_lambda_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARHYPERPARMS_HPP_
