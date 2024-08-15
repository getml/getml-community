// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_LINEARREGRESSION_HPP_
#define PREDICTORS_LINEARREGRESSION_HPP_

#include <cmath>
#include <memory>
#include <optional>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/from_named_tuple.hpp>
#include <vector>

#include "predictors/Fingerprint.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/LinearRegressionHyperparams.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorImpl.hpp"
#include "predictors/StandardScaler.hpp"

namespace predictors {

/// Linear regression predictor.
class LinearRegression : public Predictor {
 public:
  struct SaveLoad {
    rfl::Field<"learning_rate_", Float> learning_rate;
    rfl::Field<"reg_lambda_", Float> reg_lambda;
    rfl::Field<"scaler_", StandardScaler> scaler;
    rfl::Field<"weights_", std::vector<Float>> weights;
  };

  using ReflectionType = SaveLoad;

 public:
  LinearRegression(const LinearRegressionHyperparams& _hyperparams,
                   const rfl::Ref<const PredictorImpl>& _impl,
                   const std::vector<Fingerprint>& _dependencies)
      : dependencies_(_dependencies),
        hyperparams_(rfl::Ref<LinearRegressionHyperparams>::make(_hyperparams)),
        impl_(_impl) {};

  ~LinearRegression() = default;

 public:
  /// Returns an importance measure for the individual features.
  std::vector<Float> feature_importances(
      const size_t _num_features) const final;

  /// Implements the fit(...) method in scikit-learn style
  std::string fit(
      const std::shared_ptr<const logging::AbstractLogger> _logger,
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y,
      const std::optional<std::vector<IntFeature>>& _X_categorical_valid,
      const std::optional<std::vector<FloatFeature>>& _X_numerical_valid,
      const std::optional<FloatFeature>& _y_valid) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Implements the predict(...) method in scikit-learn style
  FloatFeature predict(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const final;

  /// Stores the predictor
  void save(const std::string& _fname,
            const typename helpers::Saver::Format& _format) const final;

 public:
  /// Whether the predictor accepts null values.
  bool accepts_null() const final { return false; }

  /// Returns a deep copy.
  rfl::Ref<Predictor> clone() const final {
    return rfl::make_ref<LinearRegression>(*this);
  }

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  Fingerprint fingerprint() const final {
    using LinearRegressionFingerprint =
        typename Fingerprint::LinearRegressionFingerprint;
    return Fingerprint(LinearRegressionFingerprint{
        .hyperparams = hyperparams(),
        .dependencies = dependencies_,
        .other =
            rfl::from_named_tuple<commands::Fingerprint::OtherPredRequirements>(
                impl().reflection())});
  }

  /// Whether the predictor is used for classification;
  bool is_classification() const final { return false; }

  /// Whether the predictor has been fitted.
  bool is_fitted() const final { return weights_.size() > 0; }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const {
    return ReflectionType{.learning_rate = hyperparams().learning_rate(),
                          .reg_lambda = hyperparams().reg_lambda(),
                          .scaler = scaler_,
                          .weights = weights_};
  }

  /// Whether we want the predictor to be silent.
  bool silent() const final { return true; }

  /// The type of the predictor.
  std::string type() const final {
    return LinearRegressionHyperparams::Tag().str();
  }

 private:
  /// Trivial (private const) accessor.
  const LinearRegressionHyperparams& hyperparams() const {
    return *hyperparams_;
  }

 private:
  /// Generates predictions when no categorical columns have been passed.
  FloatFeature predict_dense(
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Generates predictions when at least one categorical column has been
  /// passed.
  FloatFeature predict_sparse(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// When possible, the linear regression will be fitted
  ///  arithmetically.
  void solve_arithmetically(const std::vector<FloatFeature>& _X_numerical,
                            const FloatFeature& _y);

  /// When necessary, we will use numerical algorithms.
  void solve_numerically(const std::vector<IntFeature>& _X_categorical,
                         const std::vector<FloatFeature>& _X_numerical,
                         const FloatFeature& _y);

 private:
  /// Calculates the gradients needed for the updates.
  void calculate_gradients(const size_t _begin, const size_t _end,
                           const unsigned int* _indices, const Float* _data,
                           const Float _delta, std::vector<Float>* _gradients) {
    assert_true(_gradients->size() == weights_.size());

    for (auto ix = _begin; ix < _end; ++ix) {
      assert_true(_indices[ix] < _gradients->size());
      (*_gradients)[_indices[ix]] += _delta * _data[ix];
    }
    _gradients->back() += _delta;
  }

  /// Applies the L2 regularization term for numerical optimization.
  void calculate_regularization(const Float _bsize_float,
                                std::vector<Float>* _gradients) {
    if (hyperparams().reg_lambda() > 0.0) {
      for (size_t i = 0; i < weights_.size(); ++i) {
        (*_gradients)[i] +=
            hyperparams().reg_lambda() * weights_[i] * _bsize_float;
      }
    }
  }

  /// Trivial (private) accessor.
  const PredictorImpl& impl() const { return *impl_; }

  /// Returns a sparse prediction.
  Float predict_sparse(const size_t _begin, const size_t _end,
                       const unsigned int* _indices, const Float* _data) const {
    Float yhat = weights_.back();
    for (auto ix = _begin; ix < _end; ++ix) {
      assert_true(_indices[ix] < weights_.size());
      yhat += _data[ix] * weights_[_indices[ix]];
    }
    return yhat;
  }

 private:
  /// The dependencies used to build the fingerprint.
  std::vector<Fingerprint> dependencies_;

  /// The hyperparameters used for the LinearRegression.
  rfl::Ref<const LinearRegressionHyperparams> hyperparams_;

  /// Implementation class for member functions common to most predictors.
  rfl::Ref<const PredictorImpl> impl_;

  /// For rescaling the input data such that the standard deviation of each
  /// column is 1.0
  StandardScaler scaler_;

  /// The slopes of the linear regression.
  std::vector<Float> weights_;
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARREGRESSION_HPP_
