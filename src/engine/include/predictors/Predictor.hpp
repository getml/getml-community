// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_PREDICTOR_HPP_
#define PREDICTORS_PREDICTOR_HPP_

#include <memory>
#include <optional>
#include <vector>

#include "debug/debug.hpp"
#include "logging/AbstractLogger.hpp"
#include "predictors/Fingerprint.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"

namespace predictors {

/// Abstract base class for a predictor
class Predictor {
 public:
  Predictor(){};

  virtual ~Predictor() = default;

  /// Whether the predictor accepts null values.
  virtual bool accepts_null() const = 0;

  /// Returns a deep copy.
  virtual std::shared_ptr<Predictor> clone() const = 0;

  /// Returns an importance measure for the individual features
  virtual std::vector<Float> feature_importances(
      const size_t _num_features) const = 0;

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  virtual Fingerprint fingerprint() const = 0;

  /// Implements the fit(...) method in scikit-learn style
  virtual std::string fit(
      const std::shared_ptr<const logging::AbstractLogger> _logger,
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y,
      const std::optional<std::vector<IntFeature>>& _X_categorical_valid,
      const std::optional<std::vector<FloatFeature>>& _X_numerical_valid,
      const std::optional<FloatFeature>& _y_valid) = 0;

  /// Whether the predictor is used for classification.
  virtual bool is_classification() const = 0;

  /// Whether the predictor has already been fitted.
  virtual bool is_fitted() const = 0;

  /// Loads the predictor
  virtual void load(const std::string& _fname) = 0;

  /// Implements the predict(...) method in scikit-learn style
  virtual FloatFeature predict(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const = 0;

  /// Stores the predictor
  virtual void save(const std::string& _fname) const = 0;

  /// Whether we want the predictor to be silent.
  virtual bool silent() const = 0;

  /// The type of the predictor.
  virtual std::string type() const = 0;
};

}  // namespace predictors

#endif  // ENGINE_PREDICTORS_PREDICTOR_HPP_
