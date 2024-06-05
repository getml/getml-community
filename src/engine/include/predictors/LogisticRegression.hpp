// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_LOGISTICREGRESSION_HPP_
#define PREDICTORS_LOGISTICREGRESSION_HPP_

#include <cmath>
#include <memory>
#include <optional>
#include <vector>

#include "debug/debug.hpp"
#include "predictors/Fingerprint.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/LogisticRegressionHyperparams.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorImpl.hpp"
#include "predictors/StandardScaler.hpp"
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

namespace predictors {

/// LogisticRegression predictor.
class LogisticRegression : public Predictor {
 public:
  struct SaveLoad {
    rfl::Field<"learning_rate_", Float> learning_rate;
    rfl::Field<"reg_lambda_", Float> reg_lambda;
    rfl::Field<"scaler_", StandardScaler> scaler;
    rfl::Field<"weights_", std::vector<Float>> weights;
  };

  using ReflectionType = SaveLoad;

 public:
  LogisticRegression(const LogisticRegressionHyperparams& _hyperparams,
                     const rfl::Ref<const PredictorImpl>& _impl,
                     const std::vector<Fingerprint>& _dependencies)
      : dependencies_(_dependencies),
        hyperparams_(
            rfl::Ref<LogisticRegressionHyperparams>::make(_hyperparams)),
        impl_(_impl){};

  ~LogisticRegression() = default;

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
    return rfl::make_ref<LogisticRegression>(*this);
  }

  /// Whether the predictor is used for classification;
  bool is_classification() const final { return true; }

  /// Whether the predictor has been fitted.
  bool is_fitted() const final { return weights_.size() > 0; }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const {
    return ReflectionType{.learning_rate = hyperparams().learning_rate(),
                          .reg_lambda = hyperparams().reg_lambda(),
                          .scaler = scaler_,
                          .weights = weights_};
  }

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  Fingerprint fingerprint() const final {
    using LogisticRegressionFingerprint =
        typename Fingerprint::LogisticRegressionFingerprint;
    return Fingerprint(LogisticRegressionFingerprint{
        .hyperparams = hyperparams(),
        .dependencies = dependencies_,
        .other =
            rfl::from_named_tuple<commands::Fingerprint::OtherPredRequirements>(
                impl().reflection())});
  }

  /// Whether we want the predictor to be silent.
  bool silent() const final { return true; }

  /// The type of the predictor.
  std::string type() const final {
    return LogisticRegressionHyperparams::Tag().str();
  }

 private:
  /// Trivial (private const) accessor.
  const LogisticRegressionHyperparams& hyperparams() const {
    return *hyperparams_;
  }

 private:
  /// Fit on dense data.
  void fit_dense(const std::shared_ptr<const logging::AbstractLogger> _logger,
                 const std::vector<FloatFeature>& _X_numerical,
                 const FloatFeature& _y);

  /// Fit on sparse data.
  void fit_sparse(const std::shared_ptr<const logging::AbstractLogger> _logger,
                  const std::vector<IntFeature>& _X_categorical,
                  const std::vector<FloatFeature>& _X_numerical,
                  const FloatFeature& _y);

  /// Generates predictions when no categorical columns have been passed.
  FloatFeature predict_dense(
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Generates predictions when at least one categorical column has been
  /// passed.
  FloatFeature predict_sparse(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

 private:
  /// Calculates the gradients needed for the updates (sparse).
  const void calculate_gradients(const std::vector<FloatFeature>& _X,
                                 const size_t _i, const Float _delta,
                                 std::vector<Float>* _gradients) {
    assert_true(_gradients->size() == weights_.size());
    assert_true(_gradients->size() == _X.size() + 1);

    for (size_t j = 0; j < _X.size(); ++j) {
      (*_gradients)[j] += _delta * _X[j][_i];
    }

    _gradients->back() += _delta;
  }

  /// Calculates the gradients needed for the updates (sparse).
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

  /// Logistic function.
  Float logistic_function(const Float _x) const {
    return 1.0 / (1.0 + std::exp(-1.0 * _x));
  }

  /// Trivial (private) accessor.
  const PredictorImpl& impl() const { return *impl_; }

  /// Returns a dense prediction.
  const Float predict_dense(const std::vector<FloatFeature>& _X,
                            const size_t _i) const {
    Float yhat = weights_.back();
    for (size_t j = 0; j < _X.size(); ++j) {
      yhat += _X[j][_i] * weights_[j];
    }
    yhat = logistic_function(yhat);

    return yhat;
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
    yhat = logistic_function(yhat);
    return yhat;
  }

 private:
  /// The dependencies used to build the fingerprint.
  const std::vector<Fingerprint> dependencies_;

  /// The hyperparameters used for the LogisticRegression.
  rfl::Ref<const LogisticRegressionHyperparams> hyperparams_;

  /// Implementation class for member functions common to most predictors.
  rfl::Ref<const PredictorImpl> impl_;

  /// For rescaling the input data such that the standard deviation of each
  /// column is 1.0
  StandardScaler scaler_;

  /// The slopes of the logistic regression.
  std::vector<Float> weights_;
};

}  // namespace predictors

#endif  // PREDICTORS_LOGISTICREGRESSION_HPP_
