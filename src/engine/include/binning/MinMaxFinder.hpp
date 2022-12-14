// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef BINNING_MINMAXFINDER_HPP_
#define BINNING_MINMAXFINDER_HPP_

// ----------------------------------------------------------------------------

#include <cmath>

// ----------------------------------------------------------------------------

#include <limits>
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

template <class MatchType, class GetValueType, class VType>
struct MinMaxFinder {
  /// Finds the minimum and the maximum value that is produced by
  /// _get_value.
  static std::pair<VType, VType> find_min_max(
      const GetValueType& _get_value,
      const typename std::vector<MatchType>::const_iterator _begin,
      const typename std::vector<MatchType>::const_iterator _end,
      multithreading::Communicator* _comm);
};

// ----------------------------------------------------------------------------

template <class MatchType, class GetValueType, class VType>
std::pair<VType, VType>
MinMaxFinder<MatchType, GetValueType, VType>::find_min_max(
    const GetValueType& _get_value,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _end,
    multithreading::Communicator* _comm) {
  assert_true(_end >= _begin);

  Float min = std::numeric_limits<VType>::max();

  Float max = std::numeric_limits<VType>::lowest();

  for (auto it = _begin; it != _end; ++it) {
    const VType val = _get_value(*it);

    assert_true(!std::isnan(val) && !std::isinf(val));

    if (val < min) {
      min = val;
    }

    if (val > max) {
      max = val;
    }
  }

  multithreading::Reducer::reduce(multithreading::minimum<VType>(), &min,
                                  _comm);

  multithreading::Reducer::reduce(multithreading::maximum<VType>(), &max,
                                  _comm);

  return std::make_pair(min, max);
}

// ----------------------------------------------------------------------------
}  // namespace binning

#endif  // BINNING_MINMAXFINDER_HPP_
