// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/LinearRegression.hpp"

#include <Eigen/Dense>
#include <numeric>
#include <random>

#include "helpers/Loader.hpp"
#include "helpers/Saver.hpp"
#include "optimizers/optimizers.hpp"

namespace predictors {

std::vector<Float> LinearRegression::feature_importances(
    const size_t _num_features) const {
  if (weights_.size() == 0) {
    throw std::runtime_error(
        "Cannot retrieve feature importances! Linear Regression has "
        "not been trained!");
  }

  if (_num_features !=
      impl().num_autofeatures() + impl().num_manual_features()) {
    throw std::runtime_error(
        "Incorrect number of features when retrieving in feature "
        "importances! Expected " +
        std::to_string(impl().num_autofeatures() +
                       impl().num_manual_features()) +
        ", got " + std::to_string(_num_features) + ".");
  }

  std::vector<Float> all_feature_importances(weights_.size() - 1);

  for (size_t i = 0; i < all_feature_importances.size(); ++i) {
    all_feature_importances[i] = std::abs(weights_[i]);
  }

  std::vector<Float> feature_importances(_num_features);

  impl().compress_importances(all_feature_importances, &feature_importances);

  const auto sum = std::accumulate(feature_importances.begin(),
                                   feature_importances.end(), 0.0);

  for (auto& f : feature_importances) {
    f /= sum;
  }

  return feature_importances;
}

// -----------------------------------------------------------------------------

std::string LinearRegression::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y,
    const std::optional<std::vector<IntFeature>>& _X_categorical_valid,
    const std::optional<std::vector<FloatFeature>>& _X_numerical_valid,
    const std::optional<FloatFeature>& _y_valid) {
  impl().check_plausibility(_X_categorical, _X_numerical, _y);

  if (_X_categorical.size() > 0) {
    solve_numerically(_X_categorical, _X_numerical, _y);
  } else {
    solve_arithmetically(_X_numerical, _y);
  }

  if (_logger) {
    _logger->log("Progress: 100%.");
  }

  return "";
}

// -----------------------------------------------------------------------------

void LinearRegression::load(const std::string& _fname) {
  const auto named_tuple = helpers::Loader::load<ReflectionType>(_fname);
  scaler_ = named_tuple.scaler();
  weights_ = named_tuple.weights();
}

// -----------------------------------------------------------------------------

FloatFeature LinearRegression::predict(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical) const {
  if (!is_fitted()) {
    throw std::runtime_error("LinearRegression has not been fitted!");
  }

  impl().check_plausibility(_X_categorical, _X_numerical);

  if (_X_categorical.size() > 0) {
    return predict_sparse(_X_categorical, _X_numerical);
  } else {
    return predict_dense(_X_numerical);
  }
}

// -----------------------------------------------------------------------------

FloatFeature LinearRegression::predict_dense(
    const std::vector<FloatFeature>& _X_numerical) const {
  if (weights_.size() != _X_numerical.size() + 1) {
    throw std::runtime_error("Incorrect number of features! Expected " +
                             std::to_string(weights_.size() - 1) + ", got " +
                             std::to_string(_X_numerical.size()) + ".");
  }

  const auto X = scaler_.transform(_X_numerical);

  auto predictions =
      FloatFeature(std::make_shared<std::vector<Float>>(X[0].size()));

  for (size_t j = 0; j < X.size(); ++j) {
    assert_true(X[0].size() == X[j].size());

    for (size_t i = 0; i < X[j].size(); ++i) {
      predictions[i] += weights_[j] * X[j][i];
    }
  }

  for (size_t i = 0; i < predictions.size(); ++i) {
    predictions[i] += weights_[X.size()];
  }

  return predictions;
}

// -----------------------------------------------------------------------------

FloatFeature LinearRegression::predict_sparse(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical) const {
  auto csr_mat = impl().make_csr<Float, unsigned int, size_t>(_X_categorical,
                                                              _X_numerical);

  csr_mat = scaler_.transform(csr_mat);

  if (weights_.size() != csr_mat.ncols() + 1) {
    throw std::runtime_error(
        "Incorrect number of columns in CSRMatrix! Expected " +
        std::to_string(weights_.size() - 1) + ", got " +
        std::to_string(csr_mat.ncols()) + ".");
  }

  auto predictions =
      FloatFeature(std::make_shared<std::vector<Float>>(csr_mat.nrows()));

  for (size_t i = 0; i < csr_mat.nrows(); ++i) {
    predictions[i] =
        predict_sparse(csr_mat.indptr()[i], csr_mat.indptr()[i + 1],
                       csr_mat.indices(), csr_mat.data());
  }

  return predictions;
}

// -----------------------------------------------------------------------------

void LinearRegression::save(
    const std::string& _fname,
    const typename helpers::Saver::Format& _format) const {
  helpers::Saver::save(_fname, *this, _format);
}

// -----------------------------------------------------------------------------

void LinearRegression::solve_arithmetically(
    const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y) {
  scaler_.fit(_X_numerical);

  const auto X = scaler_.transform(_X_numerical);

  // CAREFUL: Do NOT use "auto"!
  Eigen::MatrixXd XtX = Eigen::MatrixXd(X.size() + 1, X.size() + 1);

  for (size_t i = 0; i < X.size(); ++i) {
    for (size_t j = 0; j <= i; ++j) {
      assert_true(X[i].size() == X[j].size());

      XtX(i, j) = XtX(j, i) =
          std::inner_product(X[i].begin(), X[i].end(), X[j].begin(), 0.0);
    }
  }

  for (size_t i = 0; i < X.size(); ++i) {
    XtX(X.size(), i) = XtX(i, X.size()) =
        std::accumulate(X[i].begin(), X[i].end(), 0.0);
  }

  const auto n = static_cast<Float>(_y.size());

  XtX(X.size(), X.size()) = n;

  if (hyperparams().reg_lambda() > 0.0) {
    for (size_t i = 0; i < X.size(); ++i) {
      XtX(i, i) += hyperparams().reg_lambda() * n;
    }
  }

  // CAREFUL: Do NOT use "auto"!
  Eigen::MatrixXd Xy = Eigen::MatrixXd(X.size() + 1, 1);

  for (size_t i = 0; i < X.size(); ++i) {
    assert_true(X[i].size() == _y.size());

    Xy(i) = std::inner_product(X[i].begin(), X[i].end(), _y.begin(), 0.0);
  }

  Xy(X.size()) = std::accumulate(_y.begin(), _y.end(), 0.0);

  // CAREFUL: Do NOT use "auto"!
  Eigen::MatrixXd weights = XtX.fullPivLu().solve(Xy);

  weights_.resize(X.size() + 1);

  for (size_t i = 0; i < weights_.size(); ++i) {
    weights_[i] = weights(i);
  }
}

// -----------------------------------------------------------------------------

void LinearRegression::solve_numerically(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y) {
  auto csr_mat = impl().make_csr<Float, unsigned int, size_t>(_X_categorical,
                                                              _X_numerical);

  scaler_.fit(csr_mat);

  csr_mat = scaler_.transform(csr_mat);

  std::mt19937 rng;

  std::uniform_real_distribution<> dis(-1.0, 1.0);

  weights_ = std::vector<Float>(csr_mat.ncols() + 1);

  for (auto& w : weights_) {
    w = dis(rng);
  }

  const size_t batch_size = 200;

  const auto bsize_float = static_cast<Float>(batch_size);

  std::vector<Float> gradients(weights_.size());

  auto optimizer = optimizers::Adam(hyperparams().learning_rate(), 0.999, 10.0,
                                    1e-10, weights_.size());

  for (size_t epoch = 0; epoch < 1000; ++epoch) {
    const auto epoch_float = static_cast<Float>(epoch);

    for (size_t i = 0; i < csr_mat.nrows(); ++i) {
      const auto yhat =
          predict_sparse(csr_mat.indptr()[i], csr_mat.indptr()[i + 1],
                         csr_mat.indices(), csr_mat.data());

      const auto delta = yhat - _y[i];

      calculate_gradients(csr_mat.indptr()[i], csr_mat.indptr()[i + 1],
                          csr_mat.indices(), csr_mat.data(), delta, &gradients);

      if (i % batch_size == batch_size - 1 || i == csr_mat.nrows() - 1) {
        calculate_regularization(bsize_float, &gradients);

        optimizer.update_weights(epoch_float, gradients, &weights_);

        std::fill(gradients.begin(), gradients.end(), 0.0);
      }
    }
  }
}

// -----------------------------------------------------------------------------
}  // namespace predictors
