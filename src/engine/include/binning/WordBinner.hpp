// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef BINNING_WORDBINNER_HPP_
#define BINNING_WORDBINNER_HPP_

// ----------------------------------------------------------------------------

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "binning/Float.hpp"
#include "binning/Int.hpp"

// ----------------------------------------------------------------------------

namespace binning {
// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
class WordBinner {
 public:
  static std::vector<size_t> bin(
      const std::vector<strings::String>& _vocabulary,
      const GetValueType& _get_value,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin,
      const typename std::vector<MatchType>::const_iterator _end,
      std::vector<MatchType>* _bins);

 private:
  /// Generates the indptr, which indicates the beginning and end of
  /// each bin.
  static std::vector<size_t> make_indptr(
      const std::vector<strings::String>& _vocabulary,
      const GetValueType& _get_value,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin);
};

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::vector<size_t> WordBinner<MatchType, GetValueType>::bin(
    const std::vector<strings::String>& _vocabulary,
    const GetValueType& _get_value,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin,
    const typename std::vector<MatchType>::const_iterator _end,
    std::vector<MatchType>* _bins) {
  // ---------------------------------------------------------------------------

  assert_true(_end >= _begin);

  assert_true(_bins->size() >=
              static_cast<size_t>(std::distance(_begin, _end)));

  // ---------------------------------------------------------------------------

  const auto indptr = make_indptr(_vocabulary, _get_value, _begin, _nan_begin);

  // ---------------------------------------------------------------------------

  assert_true(indptr.size() == _vocabulary.size() + 1);

  std::vector<size_t> counts(_vocabulary.size());

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto ix = _get_value(*it);

    assert_true(ix >= 0);
    assert_true(static_cast<size_t>(ix + 1) < indptr.size());

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
std::vector<size_t> WordBinner<MatchType, GetValueType>::make_indptr(
    const std::vector<strings::String>& _vocabulary,
    const GetValueType& _get_value,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _nan_begin) {
  std::vector<size_t> indptr(_vocabulary.size() + 1);

  for (auto it = _begin; it != _nan_begin; ++it) {
    const auto ix = _get_value(*it);

    assert_true(ix >= 0);
    assert_true(static_cast<size_t>(ix + 1) < indptr.size());

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

#endif  // BINNING_WORDBINNER_HPP_
