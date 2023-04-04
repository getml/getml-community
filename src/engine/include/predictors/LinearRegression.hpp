// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_LINEARREGRESSION_HPP_
#define PREDICTORS_LINEARREGRESSION_HPP_

#include <memory>
#include <optional>
#include <vector>

#include "debug/debug.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
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
  using f_scaler = fct::Field<"scaler_", StandardScaler>;
  using f_weights = fct::Field<"weights_", std::vector<Float>>;

  using NamedTupleType =
      fct::NamedTuple<fct::Field<"learning_rate_", Float>,
                      fct::Field<"reg_lambda_", Float>, f_scaler, f_weights>;

 public:
  LinearRegression(const LinearRegressionHyperparams& _hyperparams,
                   const fct::Ref<const PredictorImpl>& _impl,
                   const std::vector<Fingerprint>& _dependencies)
      : dependencies_(_dependencies),
        hyperparams_(fct::Ref<LinearRegressionHyperparams>::make(_hyperparams)),
        impl_(_impl){};

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
  void save(const std::string& _fname) const final;

 public:
  /// Whether the predictor accepts null values.
  bool accepts_null() const final { return false; }

  /// Returns a deep copy.
  std::shared_ptr<Predictor> clone() const final {
    return std::make_shared<LinearRegression>(*this);
  }

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  Fingerprint fingerprint() const final {
    using LinearRegressionFingerprint =
        typename Fingerprint::LinearRegressionFingerprint;
    return Fingerprint(LinearRegressionFingerprint(
        hyperparams().val_ * fct::make_field<"dependencies_">(dependencies_) *
        impl().named_tuple()));
  }

  /// Whether the predictor is used for classification;
  bool is_classification() const final { return false; }

  /// Whether the predictor has been fitted.
  bool is_fitted() const final { return weights_.size() > 0; }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const {
    return fct::make_field<"learning_rate_">(hyperparams().learning_rate()) *
           fct::make_field<"reg_lambda_">(hyperparams().reg_lambda()) *
           f_scaler(scaler_) * f_weights(weights_);
  }

  /// Whether we want the predictor to be silent.
  bool silent() const final { return true; }

  /// The type of the predictor.
  std::string type() const final {
    return hyperparams().val_.get<"type_">().name();
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
  const void calculate_gradients(const size_t _begin, const size_t _end,
                                 const unsigned int* _indices,
                                 const Float* _data, const Float _delta,
                                 std::vector<Float>* _gradients) {
    assert_true(_gradients->size() == weights_.size());

    for (auto ix = _begin; ix < _end; ++ix) {
      assert_true(_indices[ix] < _gradients->size());
      (*_gradients)[_indices[ix]] += _delta * _data[ix];
    }
    _gradients->back() += _delta;
  }

  /// Applies the L2 regularization term for numerical optimization.
  const void calculate_regularization(const Float _bsize_float,
                                      std::vector<Float>* _gradients) {
    if (hyperparams().reg_lambda() > 0.0) {
      for (size_t i = 0; i < weights_.size(); ++i) {
        (*_gradients)[i] +=
            hyperparams().reg_lambda() * weights_[i] * _bsize_float;
      }
    }
  }

  /// Trivial (private) accessor.
  const PredictorImpl& impl() const {
    assert_true(impl_);
    return *impl_;
  }

  /// Returns a sparse prediction.
  const Float predict_sparse(const size_t _begin, const size_t _end,
                             const unsigned int* _indices,
                             const Float* _data) const {
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
  fct::Ref<const LinearRegressionHyperparams> hyperparams_;

  /// Implementation class for member functions common to most predictors.
  fct::Ref<const PredictorImpl> impl_;

  /// For rescaling the input data such that the standard deviation of each
  /// column is 1.0
  StandardScaler scaler_;

  /// The slopes of the linear regression.
  std::vector<Float> weights_;
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARREGRESSION_HPP_
