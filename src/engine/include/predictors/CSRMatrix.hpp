// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef PREDICTORS_CONTAINERS_CSRMATRIX_HPP_
#define PREDICTORS_CONTAINERS_CSRMATRIX_HPP_

// -----------------------------------------------------------------------------

#include <algorithm>
#include <vector>

// -----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "fct/fct.hpp"

// -----------------------------------------------------------------------------

#include "predictors/Float.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/Int.hpp"
#include "predictors/IntFeature.hpp"

// -----------------------------------------------------------------------------

namespace predictors {

template <typename DataT = Float, typename IndicesT = size_t,
          typename IndptrT = size_t>
class CSRMatrix {
 public:
  using DataType = DataT;
  using IndicesType = IndicesT;
  using IndptrType = IndptrT;

 public:
  /// Constructs an empty CSRMatrix.
  CSRMatrix() : indptr_(std::vector<IndptrType>(1)), ncols_(0) {}

  /// Constructs a CSRMatrix from a discrete or numerical range.
  explicit CSRMatrix(const fct::Range<const Float*>& _col);

  /// Constructs a CSRMatrix from a categorical range.
  CSRMatrix(const fct::Range<const Int*>& _col, const size_t _n_unique);

  /// Constructs a CSRMatrix from a discrete or numerical column.
  explicit CSRMatrix(const FloatFeature& _f)
      : CSRMatrix(fct::Range(_f.begin(), _f.end())) {}

  /// Constructs a CSRMatrix from a categorical feature.
  CSRMatrix(const IntFeature& _f, const size_t _n_unique)
      : CSRMatrix(fct::Range(_f.begin(), _f.end())) {}

  // -----------------------------------------------------------

 public:
  /// Adds a discrete or numerical range.
  void add(const fct::Range<const Float*>& _col);

  /// Adds a categorical column.
  void add(const fct::Range<const Int*>& _col, const size_t _n_unique);

  // -----------------------------------------------------------

 public:
  /// Adds a discrete or numerical range.
  void add(const FloatFeature& _f) { add(fct::Range(_f.begin(), _f.end())); }

  /// Adds a categorical column.
  void add(const IntFeature& _f, const size_t _n_unique) {
    add(fct::Range(_f.begin(), _f.end()), _n_unique);
  }

  /// Deletes all data in the CSRMatrix.
  void clear() { *this = CSRMatrix(); }

  /// Trivial accessor.
  DataType* data() { return data_.data(); }

  /// Trivial (const) accessor.
  const DataType* data() const { return data_.data(); }

  /// Trivial (const) accessor.
  const IndptrType* indptr() const { return indptr_.data(); }

  /// Trivial (const) accessor.
  const IndicesType* indices() const { return indices_.data(); }

  /// Trivial (const) accessor.
  const size_t ncols() const { return ncols_; }

  /// Trivial (const) accessor.
  const size_t nrows() const {
    assert_true(indptr_.size() != 0);
    return indptr_.size() - 1;
  }

  /// Number of non-zero entries.
  const size_t size() const {
    assert_true(data_.size() == indices_.size());
    return data_.size();
  }

 private:
  /// Generates a new data_ by adding the new range.
  std::vector<DataType> update_data_from_float(
      const fct::Range<const Float*>& _col) const;

  /// Generates a new indices_ by adding the new range.
  std::vector<IndicesType> update_indices_from_float(
      const fct::Range<const Float*>& _col) const;

  /// Generates a new data_ by adding the new range.
  std::vector<DataType> update_data_from_int(
      const fct::Range<const Int*>& _col, const size_t _num_non_negative) const;

  /// Generates a new indices_ by adding the new range.
  std::vector<IndicesType> update_indices_from_int(
      const fct::Range<const Int*>& _col, const size_t _num_non_negative) const;

  /// Updates the indptr_ when adding an Int range.
  void update_indptr_from_int(const fct::Range<const Int*>& _col,
                              const size_t _num_non_negative);

  // -----------------------------------------------------------

 private:
  /// Contains the actual data.
  std::vector<DataType> data_;

  /// Pointers to where columns begin and end.
  std::vector<IndptrType> indptr_;

  /// Indicate the rows.
  std::vector<IndicesType> indices_;

  /// The number of columns in the CSRMatrix.
  size_t ncols_;

  // -----------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

namespace predictors {

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType>::CSRMatrix(
    const fct::Range<const Float*>& _col) {
  data_ = std::vector<DataType>(_col.size());

  const auto to_data = [](const auto val) {
    return static_cast<DataType>(val);
  };

  std::transform(_col.begin(), _col.end(), data_.begin(), to_data);

  for (size_t i = 0; i < data_.size(); ++i) {
    data_[i] = static_cast<DataType>(_col[i]);
  }

  indices_ = std::vector<IndicesType>(_col.size());

  indptr_ = std::vector<IndptrType>(_col.size() + 1);

  for (size_t i = 0; i < indptr_.size(); ++i) {
    indptr_[i] = static_cast<IndptrType>(i);
  }

  ncols_ = 1;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType>::CSRMatrix(
    const fct::Range<const Int*>& _col, const size_t _n_unique) {
  indptr_ = std::vector<IndptrType>(_col.size() + 1);

  for (size_t i = 0; i < _col.size(); ++i) {
    if (_col[i] >= 0) {
      indices_.push_back(static_cast<IndicesType>(_col[i]));

      indptr_[i + 1] = indptr_[i] + 1;
    } else {
      indptr_[i + 1] = indptr_[i];
    }
  }

  data_ = std::vector<DataType>(indices_.size(), 1.0);

  ncols_ = _n_unique;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
void CSRMatrix<DataType, IndicesType, IndptrType>::add(
    const fct::Range<const Float*>& _col) {
  if (ncols() == 0) {
    *this = CSRMatrix<DataType, IndicesType, IndptrType>(_col);
    return;
  }

  data_ = update_data_from_float(_col);

  indices_ = update_indices_from_float(_col);

  for (size_t i = 1; i <= nrows(); ++i) {
    indptr_[i] += i;
  }

  ++ncols_;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
void CSRMatrix<DataType, IndicesType, IndptrType>::add(
    const fct::Range<const Int*>& _col, const size_t _n_unique) {
  if (ncols() == 0) {
    *this = CSRMatrix(_col, _n_unique);
    return;
  }

  assert_true(_col.size() == nrows());

  // -------------------------------------------------------------------------

  const auto is_non_negative = [](int val) { return (val >= 0); };

  const size_t num_non_negative =
      std::count_if(_col.begin(), _col.end(), is_non_negative);

  // -------------------------------------------------------------------------

  data_ = update_data_from_int(_col, num_non_negative);

  indices_ = update_indices_from_int(_col, num_non_negative);

  update_indptr_from_int(_col, num_non_negative);

  // -------------------------------------------------------------------------

  ncols_ += _n_unique;

  // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
std::vector<DataType>
CSRMatrix<DataType, IndicesType, IndptrType>::update_data_from_float(
    const fct::Range<const Float*>& _col) const {
  auto data_temp = std::vector<DataType>(data_.size() + _col.size());

  for (size_t i = 0; i < nrows(); ++i) {
    std::copy(data_.begin() + indptr_[i], data_.begin() + indptr_[i + 1],
              data_temp.begin() + indptr_[i] + i);

    data_temp[indptr_[i + 1] + i] = static_cast<DataType>(_col[i]);
  }

  return data_temp;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
std::vector<IndicesType>
CSRMatrix<DataType, IndicesType, IndptrType>::update_indices_from_float(
    const fct::Range<const Float*>& _col) const {
  auto indices_temp = std::vector<IndicesType>(indices_.size() + _col.size());

  for (size_t i = 0; i < nrows(); ++i) {
    std::copy(indices_.begin() + indptr_[i], indices_.begin() + indptr_[i + 1],
              indices_temp.begin() + indptr_[i] + i);

    indices_temp[indptr_[i + 1] + i] = static_cast<IndicesType>(ncols_);
  }

  return indices_temp;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
std::vector<DataType>
CSRMatrix<DataType, IndicesType, IndptrType>::update_data_from_int(
    const fct::Range<const Int*>& _col, const size_t _num_non_negative) const {
  auto data_temp = std::vector<DataType>(data_.size() + _num_non_negative);

  IndptrType num_added = 0;

  for (size_t i = 0; i < nrows(); ++i) {
    std::copy(data_.begin() + indptr_[i], data_.begin() + indptr_[i + 1],
              data_temp.begin() + indptr_[i] + num_added);

    if (_col[i] >= 0) {
      data_temp[indptr_[i + 1] + num_added] = 1.0;

      ++num_added;
    }
  }

  assert_true(static_cast<size_t>(num_added) == _num_non_negative);

  return data_temp;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
std::vector<IndicesType>
CSRMatrix<DataType, IndicesType, IndptrType>::update_indices_from_int(
    const fct::Range<const Int*>& _col, const size_t _num_non_negative) const {
  auto indices_temp =
      std::vector<IndicesType>(indices_.size() + _num_non_negative);

  IndptrType num_added = 0;

  for (size_t i = 0; i < nrows(); ++i) {
    std::copy(indices_.begin() + indptr_[i], indices_.begin() + indptr_[i + 1],
              indices_temp.begin() + indptr_[i] + num_added);

    if (_col[i] >= 0) {
      indices_temp[indptr_[i + 1] + num_added] =
          static_cast<IndicesType>(_col[i]) + static_cast<IndicesType>(ncols());

      ++num_added;
    }
  }

  assert_true(static_cast<size_t>(num_added) == _num_non_negative);

  return indices_temp;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
void CSRMatrix<DataType, IndicesType, IndptrType>::update_indptr_from_int(
    const fct::Range<const Int*>& _col, const size_t _num_non_negative) {
  assert_true(_col.size() + 1 == indptr_.size());

  IndptrType num_added = 0;

  for (size_t i = 0; i < _col.size(); ++i) {
    if (_col[i] >= 0) {
      ++num_added;
    }

    indptr_[i + 1] += num_added;
  }

  assert_true(static_cast<size_t>(num_added) == _num_non_negative);
}

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_CONTAINERS_CSRMATRIX_HPP_
