// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef BINNING_CATEGORICALBINNER_HPP_
#define BINNING_CATEGORICALBINNER_HPP_

// ----------------------------------------------------------------------------

#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "binning/Float.hpp"
#include "binning/Int.hpp"

// ----------------------------------------------------------------------------

namespace binning {
// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
class CategoricalBinner {
 public:
  static std::pair<std::vector<size_t>, std::shared_ptr<const std::vector<Int>>>
  bin(const Int _min, const Int _max, const GetValueType& _get_value,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin,
      const typename std::vector<MatchType>::const_iterator _end,
      std::vector<MatchType>* _bins, multithreading::Communicator* _comm);

 private:
  /// Generates the critical values - a list of all category that have a count
  /// of at least one.
  static std::shared_ptr<const std::vector<Int>> make_critical_values(
      const Int _min, const Int _max, const GetValueType& _get_value,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin,
      multithreading::Communicator* _comm);

  /// Generates the indptr, which indicates the beginning and end of
  /// each bin.
  static std::vector<size_t> make_indptr(
      const Int _min, const Int _max, const GetValueType& _get_value,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin);
};

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::pair<std::vector<size_t>, std::shared_ptr<const std::vector<Int>>>
CategoricalBinner<MatchType, GetValueType>::bin(
    const Int _min, const Int _max, const GetValueType& _get_value,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin,
    const typename std::vector<MatchType>::const_iterator _end,
    std::vector<MatchType>* _bins, multithreading::Communicator* _comm) {
  // ---------------------------------------------------------------------------

  assert_true(_nan_begin >= _begin);
  assert_true(_end >= _nan_begin);

  assert_true(_bins->size() >=
              static_cast<size_t>(std::distance(_begin, _end)));

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case. They can be equal though, for instance
  // for same units categorical in Multirel.

  if (_min > _max) {
    return std::make_pair(std::vector<size_t>(0),
                          std::shared_ptr<const std::vector<Int>>());
  }

  // ---------------------------------------------------------------------------

  const auto num_bins = _max - _min + 1;

  const auto indptr = make_indptr(_min, _max, _get_value, _begin, _nan_begin);

  // ---------------------------------------------------------------------------

  assert_true(indptr.size() == static_cast<size_t>(num_bins) + 1);

  std::vector<size_t> counts(num_bins);

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto val = _get_value(*it);

    const auto ix = val - _min;

    assert_true(ix >= 0);
    assert_true(ix < num_bins);

    assert_true(indptr[ix] + counts[ix] < indptr[ix + 1]);

    *(_bins->begin() + indptr[ix] + counts[ix]) = *it;

    ++counts[ix];
  }

  // ---------------------------------------------------------------------------

  assert_true(indptr.size() > 0);

  std::copy(_nan_begin, _end, _bins->begin() + indptr.back());

  // ---------------------------------------------------------------------------

  const auto critical_values =
      make_critical_values(_min, _max, _get_value, _begin, _nan_begin, _comm);

  // ---------------------------------------------------------------------------

  return std::make_pair(indptr, critical_values);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::shared_ptr<const std::vector<Int>>
CategoricalBinner<MatchType, GetValueType>::make_critical_values(
    const Int _min, const Int _max, const GetValueType& _get_value,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin,
    multithreading::Communicator* _comm) {
  // ------------------------------------------------------------------------
  // Find unique categories (signified by a boolean vector). We cannot use the
  // actual boolean type, because bool is smaller than char and therefore the
  // all_reduce operator won't work. So we use std::int8_t instead.

  auto included = std::vector<std::int8_t>(_max - _min + 1, 0);

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto val = _get_value(*it);

    assert_true(val >= _min);
    assert_true(val <= _max);

    included[val - _min] = 1;
  }

  // ------------------------------------------------------------------------

  multithreading::Reducer::reduce(multithreading::maximum<std::int8_t>(),
                                  &included, _comm);

  // ------------------------------------------------------------------------

  auto categories = std::make_shared<std::vector<Int>>(0);

  for (Int i = 0; i <= _max - _min; ++i) {
    if (included[i] == 1) {
      categories->push_back(_min + i);
    }
  }

  // ------------------------------------------------------------------------

  return categories;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::vector<size_t> CategoricalBinner<MatchType, GetValueType>::make_indptr(
    const Int _min, const Int _max, const GetValueType& _get_value,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin) {
  assert_true(_max >= _min);

  const auto num_bins = _max - _min + 1;

  std::vector<size_t> indptr(num_bins + 1);

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto val = _get_value(*it);

    const auto ix = val - _min;

    assert_true(ix >= 0);
    assert_true(ix < num_bins);

    ++indptr[ix + 1];
  }

  std::partial_sum(indptr.begin(), indptr.end(), indptr.begin());

  assert_true(indptr.front() == 0);

  assert_true(indptr.back() ==
              static_cast<size_t>(std::distance(_begin, _nan_begin)));

  return indptr;
}

// ----------------------------------------------------------------------------
}  // namespace binning

#endif  // BINNING_CATEGORICALBINNER_HPP_
