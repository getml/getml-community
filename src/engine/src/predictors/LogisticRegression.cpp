// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/LogisticRegression.hpp"

#include <random>

#include "helpers/Loader.hpp"
#include "helpers/Saver.hpp"
#include "optimizers/optimizers.hpp"

namespace predictors {

std::vector<Float> LogisticRegression::feature_importances(
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

std::string LogisticRegression::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y,
    const std::optional<std::vector<IntFeature>>& _X_categorical_valid,
    const std::optional<std::vector<FloatFeature>>& _X_numerical_valid,
    const std::optional<FloatFeature>& _y_valid) {
  impl().check_plausibility(_X_categorical, _X_numerical, _y);

  if (_X_categorical.size() == 0) {
    fit_dense(_logger, _X_numerical, _y);
  } else {
    fit_sparse(_logger, _X_categorical, _X_numerical, _y);
  }

  if (_logger) {
    _logger->log("Progress: 100%.");
  }

  return "";
}

// -----------------------------------------------------------------------------

void LogisticRegression::fit_dense(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y) {
  scaler_.fit(_X_numerical);

  const auto X = scaler_.transform(_X_numerical);

  std::mt19937 rng;

  std::uniform_real_distribution<> dis(-1.0, 1.0);

  weights_ = std::vector<Float>(X.size() + 1);

  for (auto& w : weights_) {
    w = dis(rng);
  }

  const auto nrows = X[0].size();

  const auto nrows_float = static_cast<Float>(nrows);

  std::vector<Float> gradients(weights_.size());

  auto optimizer = optimizers::BFGS(1.0, weights_.size());

  bool has_converged = false;

  for (size_t epoch = 0; epoch < 1000; ++epoch) {
    const auto epoch_float = static_cast<Float>(epoch);

    for (size_t i = 0; i < nrows; ++i) {
      const auto yhat = predict_dense(X, i);

      const auto delta = yhat - _y[i];

      calculate_gradients(X, i, delta, &gradients);

      if (i == nrows - 1) {
        for (auto& g : gradients) {
          g /= nrows_float;
        }

        calculate_regularization(1.0, &gradients);

        const auto gradients_rmse = std::sqrt(std::inner_product(
            gradients.begin(), gradients.end(), gradients.begin(), 0.0));

        if (gradients_rmse < 1e-04) {
          has_converged = true;
          break;
        }

        optimizer.update_weights(epoch_float, gradients, &weights_);

        std::fill(gradients.begin(), gradients.end(), 0.0);
      }
    }

    if (has_converged) {
      break;
    }
  }
}

// -----------------------------------------------------------------------------

void LogisticRegression::fit_sparse(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
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

void LogisticRegression::load(const std::string& _fname) {
  const auto named_tuple = helpers::Loader::load<ReflectionType>(_fname);
  scaler_ = named_tuple.scaler();
  weights_ = named_tuple.weights();
}

// -----------------------------------------------------------------------------

FloatFeature LogisticRegression::predict(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical) const {
  if (!is_fitted()) {
    throw std::runtime_error("LinearRegression has not been fitted!");
  }

  impl().check_plausibility(_X_categorical, _X_numerical);

  if (_X_categorical.size() == 0) {
    return predict_dense(_X_numerical);
  } else {
    return predict_sparse(_X_categorical, _X_numerical);
  }
}

// -----------------------------------------------------------------------------

FloatFeature LogisticRegression::predict_dense(
    const std::vector<FloatFeature>& _X_numerical) const {
  if (weights_.size() != _X_numerical.size() + 1) {
    throw std::runtime_error("Incorrect number of features! Expected " +
                             std::to_string(weights_.size() - 1) + ", got " +
                             std::to_string(_X_numerical.size()) + ".");
  }

  const auto X = scaler_.transform(_X_numerical);

  assert_true(X.size() > 0);

  auto predictions =
      FloatFeature(std::make_shared<std::vector<Float>>(X.at(0).size()));

  for (size_t j = 0; j < X.size(); ++j) {
    assert_true(X[0].size() == X[j].size());

    for (size_t i = 0; i < X[j].size(); ++i) {
      predictions[i] += weights_[j] * X[j][i];
    }
  }

  for (auto& yhat : predictions) {
    yhat += weights_.back();
    yhat = logistic_function(yhat);
  }

  return predictions;
}

// -----------------------------------------------------------------------------

FloatFeature LogisticRegression::predict_sparse(
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

void LogisticRegression::save(
    const std::string& _fname,
    const typename helpers::Saver::Format& _format) const {
  helpers::Saver::save(_fname, *this, _format);
}

// -----------------------------------------------------------------------------
}  // namespace predictors
