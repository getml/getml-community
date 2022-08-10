// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FCT_RANGE_HPP_
#define FCT_RANGE_HPP_

#include <iterator>
#include <type_traits>

#include "debug/debug.hpp"

namespace fct {

template <class iterator>
class Range {
 public:
  Range(iterator _begin, iterator _end) : begin_(_begin), end_(_end) {}

  ~Range() = default;

  /// Trivial (const) accessor.
  iterator begin() const { return begin_; }

  /// Trivial (const) accessor.
  iterator end() const { return end_; }

  /// Access operator
  auto operator[](size_t _i) const {
    assert_true(_i < size());
    auto it = begin();
    std::advance(it, _i);
    return *it;
  }

  /// The distance between begin() and end().
  size_t size() const {
    const auto d = std::distance(begin_, end_);
    assert_true(d >= 0);
    return static_cast<size_t>(d);
  }

 private:
  /// Iterator to the beginning of the rownums
  iterator begin_;

  /// Iterator to the end of the rownums
  iterator end_;
};

}  // namespace fct

#endif  // FCT_RANGE_HPP_
