// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef BINNING_DISCRETEBINNER_HPP_
#define BINNING_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------

#include <cmath>

// ----------------------------------------------------------------------------

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
#include "binning/NumericalBinner.hpp"

// ----------------------------------------------------------------------------

namespace binning {
// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
class DiscreteBinner {
 public:
  /// Bins the matches into _num_bins equal-width bins.
  /// The bins will be written into _bins_begin and the
  /// method returns and indptr to them.
  /// This assumes that min and max are known.
  static std::pair<std::vector<size_t>, Float> bin(
      const Float _min, const Float _max, const GetValueType& _get_value,
      const size_t _num_bins_numerical,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _nan_begin,
      const typename std::vector<MatchType>::const_iterator _end,
      std::vector<MatchType>* _bins);
};

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType>
std::pair<std::vector<size_t>, Float>
DiscreteBinner<MatchType, GetValueType>::bin(
    const Float _min, const Float _max, const GetValueType& _get_value,
    const size_t _num_bins_numerical,
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

  if (_min >= _max || _num_bins_numerical == 0) {
    return std::make_pair(std::vector<size_t>(0), 0.0);
  }

  // ---------------------------------------------------------------------------
  // If the number of bins is too large, then use numerical binning.

  assert_true(_max >= _min);

  const auto step_size =
      std::ceil((_max - _min) / static_cast<Float>(_num_bins_numerical));

  const auto indptr =
      NumericalBinner<MatchType, GetValueType>::bin_given_step_size(
          _min, _max, _get_value, step_size, _begin, _nan_begin, _end, _bins);

  // ---------------------------------------------------------------------------

  return std::make_pair(indptr, step_size);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace binning

#endif  // BINNING_DISCRETEBINNER_HPP_
