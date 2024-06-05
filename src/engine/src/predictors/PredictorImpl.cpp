// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/PredictorImpl.hpp"

#include <thread>

#include "helpers/Saver.hpp"
#include <rfl/json.hpp>

namespace predictors {

PredictorImpl::PredictorImpl(
    const std::vector<size_t>& _num_autofeatures,
    const std::vector<std::string>& _categorical_colnames,
    const std::vector<std::string>& _numerical_colnames)
    : categorical_colnames_(_categorical_colnames),
      numerical_colnames_(_numerical_colnames) {
  for (const auto n : _num_autofeatures) {
    autofeatures_.emplace_back(std::vector<size_t>(n));

    for (size_t i = 0; i < n; ++i) {
      autofeatures_.back().at(i) = i;
    }
  }
};

// -----------------------------------------------------------------------------

PredictorImpl::PredictorImpl(const ReflectionType& _nt)
    : autofeatures_(_nt.get<f_autofeatures>()),
      categorical_colnames_(_nt.get<f_categorical>()),
      encodings_(_nt.get<f_encoding>()),
      numerical_colnames_(_nt.get<f_numerical>()) {}

// -----------------------------------------------------------------------------

PredictorImpl::~PredictorImpl() = default;

// -----------------------------------------------------------------------------

void PredictorImpl::compress_importances(
    const std::vector<Float>& _all_feature_importances,
    std::vector<Float>* _feature_importances) const {
  assert_true(_all_feature_importances.size() == ncols_csr());

  assert_true(_feature_importances->size() ==
              num_autofeatures() + num_manual_features());

  const auto n_dense = num_autofeatures() + numerical_colnames_.size();

  std::copy(_all_feature_importances.begin(),
            _all_feature_importances.begin() + n_dense,
            _feature_importances->begin());

  auto begin = _all_feature_importances.begin() + n_dense;

  assert_true(encodings_.size() == categorical_colnames_.size() ||
              encodings_.size() == 0);

  for (size_t i = 0; i < encodings_.size(); ++i) {
    auto end = begin + encodings_[i].n_unique();

    auto imp = std::accumulate(begin, end, 0.0);

    (*_feature_importances)[n_dense + i] = imp;

    begin = end;
  }
}

// -------------------------------------------------------------------------

size_t PredictorImpl::check_plausibility(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical) const {
  if (_X_categorical.size() == 0 && _X_numerical.size() == 0) {
    throw std::runtime_error("You must provide at least one input column!");
  }

  size_t expected_size = 0;

  if (_X_categorical.size() > 0) {
    expected_size = _X_categorical[0].size();
  } else {
    expected_size = _X_numerical[0].size();
  }

  for (const auto& X : _X_categorical) {
    if (X.size() != expected_size) {
      throw std::runtime_error(
          "All input columns must have the same "
          "length!");
    }
  }

  for (const auto& X : _X_numerical) {
    if (X.size() != expected_size) {
      throw std::runtime_error("All input columns must have the same length!");
    }
  }

  return expected_size;
}

// -------------------------------------------------------------------------

void PredictorImpl::check_plausibility(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical,
    const FloatFeature& _y) const {
  const auto expected_size = check_plausibility(_X_categorical, _X_numerical);

  if (_y.size() != expected_size) {
    throw std::runtime_error(
        "Length of targets must be the same as the length of the input "
        "columns!");
  }
}

// -----------------------------------------------------------------------------

void PredictorImpl::fit_encodings(
    const std::vector<IntFeature>& _X_categorical) {
  encodings_.clear();

  for (auto& col : _X_categorical) {
    encodings_.push_back(Encoding());

    encodings_.back().fit(col);
  }
}

// -----------------------------------------------------------------------------

Int PredictorImpl::get_num_threads(const Int _num_threads) const {
  auto num_threads = _num_threads;

  if (num_threads <= 0) {
    num_threads =
        std::max(2, static_cast<Int>(std::thread::hardware_concurrency()) / 2);
  }

  return num_threads;
}

// -------------------------------------------------------------------------

void PredictorImpl::save(const std::string& _fname,
                         const typename helpers::Saver::Format _format) const {
  helpers::Saver::save(_fname, *this, _format);
}

// ----------------------------------------------------------------------------

void PredictorImpl::select_features(const size_t _n_selected,
                                    const std::vector<size_t>& _index) {
  encodings_.clear();

  auto num_preceding = num_autofeatures() + numerical_colnames_.size();

  categorical_colnames_ =
      select_cols(_n_selected, _index, num_preceding, categorical_colnames_);

  num_preceding -= numerical_colnames_.size();

  numerical_colnames_ =
      select_cols(_n_selected, _index, num_preceding, numerical_colnames_);

  const auto size = autofeatures_.size();

  if (size == 0) {
    return;
  }

  for (size_t i = 0; i < size; ++i) {
    auto& vec = autofeatures_.at(size - 1 - i);

    assert_true(vec.size() <= num_preceding);

    num_preceding -= vec.size();

    vec = select_cols(_n_selected, _index, num_preceding, vec);
  }
}

// -----------------------------------------------------------------------------

std::vector<IntFeature> PredictorImpl::transform_encodings(
    const std::vector<IntFeature>& _X_categorical) const {
  if (_X_categorical.size() != encodings_.size()) {
    const auto msg = "Expected " + std::to_string(encodings_.size()) +
                     " categorical columns, got " +
                     std::to_string(_X_categorical.size()) + ".";
    assert_msg(false, msg);
    throw std::runtime_error(msg);
  }

  if (_X_categorical.size() == 0) {
    return {};
  }

  const auto pool = _X_categorical.at(0).pool()
                        ? std::make_shared<memmap::Pool>(
                              _X_categorical.at(0).pool()->temp_dir())
                        : _X_categorical.at(0).pool();

  std::vector<IntFeature> transformed(encodings_.size());

  for (size_t i = 0; i < encodings_.size(); ++i) {
    transformed[i] = encodings_[i].transform(_X_categorical[i], pool);
  }

  return transformed;
}

// -----------------------------------------------------------------------------
}  // namespace predictors
