// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_TSINDEX_KEY_HPP_
#define CONTAINERS_TSINDEX_KEY_HPP_

#include "debug/assert_true.hpp"
#include "tsindex/Float.hpp"
#include "tsindex/Int.hpp"

#include <cmath>

namespace tsindex {
struct Key {
  /// The join key used.
  const Int join_key_;

  /// The time stamp forming the lower bound, NAN if not available.
  const Float time_stamp_;

  /// Check equality
  friend inline bool operator==(const Key& _a, const Key& _b) {
    assert_true(_a.join_key_ >= 0);
    assert_true(_b.join_key_ >= 0);
    assert_true(!std::isnan(_a.time_stamp_));
    assert_true(!std::isnan(_b.time_stamp_));
    return _a.join_key_ == _b.join_key_ && _a.time_stamp_ == _b.time_stamp_;
  };

  /// Smaller than operator.
  friend inline bool operator<(const Key& _a, const Key& _b) {
    assert_true(_a.join_key_ >= 0);
    assert_true(_b.join_key_ >= 0);
    assert_true(!std::isnan(_a.time_stamp_));
    assert_true(!std::isnan(_b.time_stamp_));
    if (_a.join_key_ != _b.join_key_) {
      return _a.join_key_ < _b.join_key_;
    }
    return _a.time_stamp_ < _b.time_stamp_;
  };

  /// Inequality operator.
  friend inline bool operator!=(const Key& _a, const Key& _b) {
    return !(_a == _b);
  }

  /// Greater than opeator
  friend inline bool operator>(const Key& _a, const Key& _b) { return _b < _a; }

  /// Smaller-equal operator.
  friend inline bool operator<=(const Key& _a, const Key& _b) {
    return !(_b < _a);
  }

  friend inline bool operator>=(const Key& _a, const Key& _b) {
    return !(_a < _b);
  }
};
}  // namespace tsindex

// ----------------------------------------------------------------------

#endif  // CONTAINERS_TSINDEX_KEY_HPP_
