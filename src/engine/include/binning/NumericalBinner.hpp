// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef BINNING_NUMERICALBINNER_HPP_
#define BINNING_NUMERICALBINNER_HPP_

// ----------------------------------------------------------------------------

#include <cmath>

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
class NumericalBinner {
 public:
  /// Bins the matches into _num_bins equal-width bins.
  /// The bins will be written into _bins_begin and the
  /// method returns and indptr to them as well as the
  /// calculated step size.
  static std::pair<std::vector<size_t>, Float> bin(
      const Float _min, const Float _max, const GetValueType& _get_value,
      const size_t _num_bins,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin,
      const typename std::vector<MatchType>::const_iterator _end,
      std::vector<MatchType>* _bins);

  /// Bins under the assumption that the step size is known.
  static std::vector<size_t> bin_given_step_size(
      const Float _min, const Float _max, const GetValueType& _get_value,
      const Float _step_size,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin,
      const typename std::vector<MatchType>::const_iterator _end,
      std::vector<MatchType>* _bins);

 private:
  /// Generates the indptr, which indicates the beginning and end of
  /// each bin.
  static std::vector<size_t> make_indptr(
      const Float _min, const Float _max, const GetValueType& _get_value,
      const Float _step_size,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin);
};

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::pair<std::vector<size_t>, Float>
NumericalBinner<MatchType, GetValueType>::bin(
    const Float _min, const Float _max, const GetValueType& _get_value,
    const size_t _num_bins,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin,
    const typename std::vector<MatchType>::const_iterator _end,
    std::vector<MatchType>* _bins) {
  // ---------------------------------------------------------------------------

  assert_true(!std::isnan(_max));
  assert_true(!std::isnan(_max));
  assert_true(!std::isinf(_min));
  assert_true(!std::isinf(_min));

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (_min >= _max || _num_bins == 0) {
    return std::make_pair(std::vector<size_t>(0), 0.0);
  }

  // ---------------------------------------------------------------------------

  const auto step_size = (_max - _min) / static_cast<size_t>(_num_bins);

  // ---------------------------------------------------------------------------

  const auto indptr = bin_given_step_size(_min, _max, _get_value, step_size,
                                          _begin, _nan_begin, _end, _bins);

  // ---------------------------------------------------------------------------

  return std::make_pair(indptr, step_size);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::vector<size_t>
NumericalBinner<MatchType, GetValueType>::bin_given_step_size(
    const Float _min, const Float _max, const GetValueType& _get_value,
    const Float _step_size,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin,
    const typename std::vector<MatchType>::const_iterator _end,
    std::vector<MatchType>* _bins) {
  // ---------------------------------------------------------------------------

  assert_true(!std::isnan(_max));
  assert_true(!std::isnan(_max));
  assert_true(!std::isinf(_min));
  assert_true(!std::isinf(_min));

  assert_true(_nan_begin >= _begin);
  assert_true(_end >= _nan_begin);

  assert_true(_bins->size() >=
              static_cast<size_t>(std::distance(_begin, _end)));

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (_min >= _max || _step_size <= 0.0) {
    return std::vector<size_t>(0);
  }

  // ---------------------------------------------------------------------------

  const auto indptr =
      make_indptr(_min, _max, _get_value, _step_size, _begin, _nan_begin);

  // ---------------------------------------------------------------------------

  assert_true(indptr.size() > 0);

  std::vector<size_t> counts(indptr.size() - 1);

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto val = _get_value(*it);

    assert_true(!std::isnan(val) && !std::isinf(val));

    assert_true(val <= _max);

    assert_true(val >= _min);

    const auto ix = static_cast<size_t>((_max - val) / _step_size);

    assert_true(ix < counts.size());

    assert_true(indptr[ix] + counts[ix] < indptr[ix + 1]);

    *(_bins->begin() + indptr[ix] + counts[ix]) = *it;

    ++counts[ix];
  }

  // ---------------------------------------------------------------------------

  assert_true(indptr.size() > 0);

  std::copy(_nan_begin, _end, _bins->begin() + indptr.back());

  // ---------------------------------------------------------------------------

  return indptr;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::vector<size_t> NumericalBinner<MatchType, GetValueType>::make_indptr(
    const Float _min, const Float _max, const GetValueType& _get_value,
    const Float _step_size,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin) {
  assert_true(_max >= _min);

  assert_true(_step_size > 0.0);

  const auto num_bins = static_cast<size_t>((_max - _min) / _step_size) + 1;

  std::vector<size_t> indptr(num_bins + 1);

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto val = _get_value(*it);

    assert_true(!std::isnan(val) && !std::isinf(val));

    assert_true(val <= _max);

    assert_true(val >= _min);

    const auto ix = static_cast<size_t>((_max - val) / _step_size);

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

#endif  // BINNING_NUMERICALBINNER_HPP_
