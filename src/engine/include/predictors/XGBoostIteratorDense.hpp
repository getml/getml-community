// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_XGBOOSTITERATORDENSE_HPP_
#define PREDICTORS_XGBOOSTITERATORDENSE_HPP_

// ------------------------------------------------------------------------

#include <xgboost/c_api.h>

// ------------------------------------------------------------------------

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

// ------------------------------------------------------------------------

#include "predictors/FloatFeature.hpp"

// ------------------------------------------------------------------------

namespace predictors {

/// The XGBoostIteratorDense class is used for iterating through the
/// memory-mapped features.
class XGBoostIteratorDense {
 private:
  static constexpr int CONTINUE = 1;
  static constexpr int END_IS_REACHED = 0;
  static constexpr int XGBOOST_SUCCESS = 0;
  static constexpr int XGBOOST_TYPE_FLOAT = 1;

 public:
  typedef std::function<void(DMatrixHandle *)> DMatrixDestructor;
  typedef std::unique_ptr<DMatrixHandle, DMatrixDestructor> DMatrixPtr;

 public:
  XGBoostIteratorDense(const std::vector<FloatFeature> &_X_numerical,
                       const std::optional<FloatFeature> &_y,
                       const std::shared_ptr<memmap::Pool> &_pool);

  ~XGBoostIteratorDense();

 public:
  /// Moves to the next batch.
  int next();

 public:
  /// Frees a DMatrixHandle pointer
  static void delete_dmatrix(DMatrixHandle *_ptr) {
    XGDMatrixFree(*_ptr);
    delete _ptr;
  }

  /// Trivial accessor
  DMatrixHandle &proxy() {
    assert_true(proxy_);
    return *proxy_;
  }

  /// Resets cur_it_ to 0.
  void reset() { cur_it_ = 0; }

 private:
  /// Calculates the batch size.
  static size_t calc_batch_size();

  /// Calculates the number of features.
  static size_t calc_num_batches(const size_t _batch_size, const size_t _nrows);

  /// Initializes the features
  static std::shared_ptr<memmap::Vector<float>> init_features(
      const std::vector<FloatFeature> &_X_numerical,
      const std::shared_ptr<memmap::Pool> &_pool);

  /// Infers the number of rows.
  static size_t init_nrows(const std::vector<FloatFeature> &_X_numerical);

  /// Initializes the proxy_ matrix.
  static DMatrixPtr init_proxy();

  /// Initializes the targets
  static std::shared_ptr<memmap::Vector<float>> init_targets(
      const std::optional<FloatFeature> &_y,
      const std::shared_ptr<memmap::Pool> &_pool);

  /// Update array_ to reflect the most recent batch.
  char *update_array();

 private:
  /// Calculates the size of the current batch.
  size_t current_batch_size() const {
    return std::min(batch_size_, nrows_ - cur_it_ * batch_size_);
  }

  /// Points to the current batch of target variables
  float *current_feature_batch() const {
    assert_true(features_);
    assert_true(cur_it_ * num_features_ * batch_size_ < features_->size());
    return features_->data() + cur_it_ * num_features_ * batch_size_;
  }

  /// Points to the current batch of target variables
  float *current_target_batch() const {
    assert_true(targets_);
    assert_true(cur_it_ * batch_size_ < targets_->size());
    return targets_->data() + cur_it_ * batch_size_;
  }

 private:
  /// The char array containing the JSON string.
  char array_[128];

  /// The size of a batch (the last batch might be smaller than that).
  const size_t batch_size_;

  /// Current iteration.
  size_t cur_it_;

  /// The features on which to train.
  const std::shared_ptr<memmap::Vector<float>> features_;

  /// The number of rows.
  const size_t nrows_;

  /// Total number of batches.
  const size_t num_batches_;

  /// The number of features.
  const size_t num_features_;

  /// The proxy matrix, functioning as the current batch.
  const DMatrixPtr proxy_;

  /// The targets used.
  const std::shared_ptr<memmap::Vector<float>> targets_;
};

// ------------------------------------------------------------------------

inline int XGBoostIteratorDense__next(DataIterHandle _handle) {
  auto iter = reinterpret_cast<XGBoostIteratorDense *>(_handle);
  return iter->next();
}

// ------------------------------------------------------------------------

inline void XGBoostIteratorDense__reset(DataIterHandle _handle) {
  auto iter = reinterpret_cast<XGBoostIteratorDense *>(_handle);
  iter->reset();
}

// ------------------------------------------------------------------------

}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTITERATORDENSE_HPP_
