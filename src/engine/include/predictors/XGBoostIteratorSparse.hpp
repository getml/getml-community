// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef PREDICTORS_XGBOOSTITERATORSPARSE_HPP_
#define PREDICTORS_XGBOOSTITERATORSPARSE_HPP_

// ------------------------------------------------------------------------

#include <unistd.h>
#include <xgboost/c_api.h>

// ------------------------------------------------------------------------

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

// ------------------------------------------------------------------------

#include "memmap/memmap.hpp"

// ------------------------------------------------------------------------

#include "predictors/CSRMatrix.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/PredictorImpl.hpp"
#include "predictors/XGBoostIteratorDense.hpp"

// ------------------------------------------------------------------------

namespace predictors {

/// The XGBoostIteratorSparse class is used for iterating through the
/// memory-mapped features.
class XGBoostIteratorSparse {
 private:
  static constexpr int ARRAY_SIZE = 128;
  static constexpr int CONTINUE = 1;
  static constexpr int END_IS_REACHED = 0;
  static constexpr int XGBOOST_SUCCESS = 0;
  static constexpr int XGBOOST_TYPE_FLOAT = 1;

 public:
  typedef XGBoostIteratorDense::DMatrixDestructor DMatrixDestructor;
  typedef XGBoostIteratorDense::DMatrixPtr DMatrixPtr;

  typedef CSRMatrix<float, std::uint32_t, std::uint64_t> CSRMatrixType;

 public:
  XGBoostIteratorSparse(const std::vector<IntFeature> &_X_categorical,
                        const std::vector<FloatFeature> &_X_numerical,
                        const std::optional<FloatFeature> &_y,
                        const std::shared_ptr<const PredictorImpl> &_impl);

  ~XGBoostIteratorSparse();

 public:
  /// Moves to the next batch.
  int next();

 public:
  /// Frees a DMatrixHandle pointer
  static void delete_dmatrix(DMatrixHandle *_ptr) {
    XGBoostIteratorDense::delete_dmatrix(_ptr);
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

  /// Calculates the number of features from _features and _targets.
  static size_t calc_num_features(
      const std::shared_ptr<memmap::Vector<float>> &_features,
      const size_t _nrows);

  /// Initializes nrows_;
  static size_t init_nrows(const std::vector<IntFeature> &_X_categorical);

  /// Initializes the proxy_ matrix.
  static DMatrixPtr init_proxy();

  /// Generates a new batch of target variables.
  std::vector<float> make_current_target_batch() const;

  /// Generates the array interfaces.
  std::tuple<const char *, const char *, const char *> make_array_interfaces(
      const CSRMatrixType &_proxy_csr);

  /// Generates the proxy CSR matrix for the current batch.
  std::unique_ptr<const CSRMatrixType> make_proxy_csr() const;

 private:
  /// Calculates the size of the current batch.
  size_t current_batch_size() const {
    return std::min(batch_size_, nrows_ - cur_it_ * batch_size_);
  }

  /// Trivial accessor for the impl
  const PredictorImpl &impl() const {
    assert_true(impl_);
    return *impl_;
  }

  /// Updates an array for the indptr, indices or data
  template <class T>
  void update_array(const T *_ptr, const size_t _size,
                    const std::string &_typestr, char *_array) const {
    const char format[] =
        "{\"data\": [%lu, false], \"shape\": [%lu], \"typestr\": "
        "\"%s\", \"version\": 3}";
    memset(_array, '\0', ARRAY_SIZE);
    sprintf(_array, format, (size_t)(_ptr), _size, _typestr.c_str());
  }

 private:
  /// The char array containing the JSON string for the data_
  char array_data_[ARRAY_SIZE];

  /// The char array containing the JSON string for the data_
  char array_indices_[ARRAY_SIZE];

  /// The char array containing the JSON string for the indptr_
  char array_indptr_[ARRAY_SIZE];

  /// The size of a batch (the last batch might be smaller than that).
  const size_t batch_size_;

  /// Current iteration.
  size_t cur_it_;

  /// We need the impl to create the CSRMatrix on-the-fly.
  const std::shared_ptr<const PredictorImpl> impl_;

  /// The number of rows.
  const size_t nrows_;

  /// Total number of batches.
  const size_t num_batches_;

  /// The proxy matrix, functioning as the current batch.
  const DMatrixPtr proxy_;

  /// The proxy CSR matrix, functioning as the current batch.
  std::unique_ptr<const CSRMatrixType> proxy_csr_;

  /// The categorical features.
  const std::vector<IntFeature> X_categorical_;

  /// The numerical features.
  const std::vector<FloatFeature> X_numerical_;

  /// The target values, if any.
  const std::optional<FloatFeature> y_;
};

// ------------------------------------------------------------------------

inline int XGBoostIteratorSparse__next(DataIterHandle _handle) {
  auto iter = reinterpret_cast<XGBoostIteratorSparse *>(_handle);
  return iter->next();
}

// ------------------------------------------------------------------------

inline void XGBoostIteratorSparse__reset(DataIterHandle _handle) {
  auto iter = reinterpret_cast<XGBoostIteratorSparse *>(_handle);
  iter->reset();
}

// ------------------------------------------------------------------------

}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTITERATORSPARSE_HPP_
