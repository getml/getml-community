// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/StandardScaler.hpp"

#include <cmath>
#include <numeric>

namespace predictors {

StandardScaler::StandardScaler()
    : val_(ReflectionType{.mean = std::vector<Float>(),
                          .std = std::vector<Float>()}) {};

StandardScaler::StandardScaler(const ReflectionType& _val) : val_(_val) {}
// -----------------------------------------------------------------------------

void StandardScaler::fit(const std::vector<FloatFeature>& _X_numerical) {
  mean().resize(_X_numerical.size());

  std().resize(_X_numerical.size());

  for (size_t j = 0; j < _X_numerical.size(); ++j) {
    const auto n = static_cast<Float>(_X_numerical[j].size());

    const auto mean =
        std::accumulate(_X_numerical[j].begin(), _X_numerical[j].end(), 0.0) /
        n;

    auto mult = [mean, n](Float val1, Float val2) {
      return (val1 - mean) * (val2 - mean) / n;
    };

    std()[j] = std::inner_product(
        _X_numerical[j].begin(), _X_numerical[j].end(), _X_numerical[j].begin(),
        0.0, std::plus<Float>(), mult);

    this->mean()[j] = mean;

    std()[j] = std::sqrt(std()[j]);
  }
}

// -----------------------------------------------------------------------------

void StandardScaler::fit(
    const CSRMatrix<Float, unsigned int, size_t>& _X_sparse) {
  // -------------------------------------------------------------------------

  const auto n = static_cast<Float>(_X_sparse.nrows());

  // -------------------------------------------------------------------------
  // Calculate means

  auto means = std::vector<Float>(_X_sparse.ncols());

  for (size_t i = 0; i < _X_sparse.nrows(); ++i) {
    for (size_t j = _X_sparse.indptr()[i]; j < _X_sparse.indptr()[i + 1]; ++j) {
      assert_true(_X_sparse.indices()[j] < _X_sparse.ncols());

      means[_X_sparse.indices()[j]] += _X_sparse.data()[j];
    }
  }

  for (auto& m : means) {
    m /= n;
  }

  // -------------------------------------------------------------------------
  // Calculate std()

  std().resize(_X_sparse.ncols());

  for (size_t i = 0; i < _X_sparse.nrows(); ++i) {
    for (size_t j = _X_sparse.indptr()[i]; j < _X_sparse.indptr()[i + 1]; ++j) {
      assert_true(_X_sparse.indices()[j] < _X_sparse.ncols());

      auto diff = (_X_sparse.data()[j] - means[_X_sparse.indices()[j]]);

      std()[_X_sparse.indices()[j]] += diff * diff;
    }
  }

  for (auto& s : std()) {
    s /= n;
    s = std::sqrt(s);
  }

  // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

std::vector<FloatFeature> StandardScaler::transform(
    const std::vector<FloatFeature>& _X_numerical) const {
  assert_true(_X_numerical.size() > 0);

  if (_X_numerical.size() != std().size()) {
    throw std::runtime_error(
        "Size of standard deviation in standard scaler does not "
        "match!");
  }

  if (_X_numerical.size() != mean().size()) {
    throw std::runtime_error(
        "Standard scaler seems to have been trained using sparse data, "
        "but is not expected to transform dense data.");
  }

  std::vector<FloatFeature> output;

  for (size_t j = 0; j < _X_numerical.size(); ++j) {
    output.push_back(FloatFeature(
        std::make_shared<std::vector<Float>>(_X_numerical[j].size())));

    if (std()[j] != 0.0) {
      for (size_t i = 0; i < _X_numerical[j].size(); ++i) {
        output.back()[i] = (_X_numerical[j][i] - mean()[j]) / std()[j];
      }
    }
  }

  return output;
}

// -----------------------------------------------------------------------------

const CSRMatrix<Float, unsigned int, size_t> StandardScaler::transform(
    const CSRMatrix<Float, unsigned int, size_t>& _X_sparse) const {
  auto output = _X_sparse;

  for (size_t i = 0; i < output.nrows(); ++i) {
    for (size_t j = output.indptr()[i]; j < output.indptr()[i + 1]; ++j) {
      assert_true(output.indices()[j] < output.ncols());

      const auto std = this->std()[_X_sparse.indices()[j]];

      if (std != 0.0) {
        output.data()[j] /= std;
      } else {
        output.data()[j] = 0.0;
      }
    }
  }

  return output;
}

// -------------------------------------------------------------------------
}  // namespace predictors
