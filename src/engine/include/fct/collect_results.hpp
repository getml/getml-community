// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_COLLECT_RESULTS_HPP_
#define FCT_COLLECT_RESULTS_HPP_

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "fct/Result.hpp"
#include "fct/ranges.hpp"
#include "multithreading/multithreading.hpp"

namespace fct {
namespace collect_results {

/// Expects a range containing Result<std::pair<K, V>>. Returns a
/// std::map<K, V> if all results are true, or Error otherwise.
template <class RangeType>
auto map(RangeType range) {
  using PairType = typename RANGES::range_value_t<RangeType>::ValueType;
  using KeyType = typename PairType::first_type;
  using ValueType = typename PairType::second_type;

  std::map<KeyType, ValueType> m;

  for (auto r : range) {
    if (!r) {
      return Result<std::map<KeyType, ValueType>>(*r.error());
    }
    const auto& [k, v] = *r;
    m[k] = v;
  }

  return Result<std::map<KeyType, ValueType>>(m);
}

/// Expects a range containing Result<T>. Returns a vector<T> if
/// all results are true, or Error otherwise.
template <class RangeType>
auto set(RangeType range) {
  using T = typename RANGES::range_value_t<RangeType>::ValueType;

  auto s = std::set<T>();

  for (auto val : range) {
    if (!val) {
      return Result<std::set<T>>(*val.error());
    }
    s.insert(*val);
  }

  return Result<std::set<T>>(s);
}

/// Expects a range containing Result<T>. Returns a vector<T> if
/// all results are true, or Error otherwise.
template <class RangeType>
auto vector(RangeType range) {
  using T = typename RANGES::range_value_t<RangeType>::ValueType;

  auto vec = std::vector<T>();

  for (auto val : range) {
    if (!val) {
      return Result<std::vector<T>>(*val.error());
    }
    vec.emplace_back(*val);
  }

  return Result<std::vector<T>>(vec);
}

}  // namespace collect_results
}  // namespace fct

#endif  // FCT_COLLECT_PARALLEL_HPP_
