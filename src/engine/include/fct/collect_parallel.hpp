// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_COLLECT_PARALLEL_HPP_
#define FCT_COLLECT_PARALLEL_HPP_

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "fct/ranges.hpp"
#include "multithreading/multithreading.hpp"

namespace fct {

namespace collect_parallel {

/// Generates a map from a range, in parallel.
template <class KeyType, class ValueType, class RangeType>
std::map<KeyType, ValueType> map(RangeType range) {
  auto m = std::map<KeyType, ValueType>();

  std::mutex mtx;

  const auto add_to_map = [&m, &mtx](const auto _pair) {
    std::lock_guard<std::mutex> guard(mtx);
    m.insert(_pair);
  };

  multithreading::parallel_for_each(range.begin(), range.end(), add_to_map);

  return m;
}

/// Generates a vector from a range, in parallel. Requires that the size of
/// the range be known in advance and random access is possible.
template <class RangeType>
auto vector(RangeType _range) {
  using T = RANGES::range_value_t<RangeType>;

  const auto size = RANGES::size(_range);

  auto vec = std::vector<T>(size);

  const auto assign = [&vec, &_range](size_t _i) {
    vec[_i] = *(_range.begin() + _i);
  };

  const auto iota_range = iota<size_t>(0, size);

  multithreading::parallel_for_each(iota_range.begin(), iota_range.end(),
                                    assign);

  return vec;
}

}  // namespace collect_parallel
}  // namespace fct

#endif  // FCT_COLLECT_PARALLEL_HPP_
