// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_NULLCHECKER_HPP_
#define HELPERS_NULLCHECKER_HPP_

#include "helpers/Float.hpp"
#include "helpers/Int.hpp"
#include "strings/String.hpp"

#include <cmath>
#include <string>

namespace helpers {

struct NullChecker {
  /// When ever we us integers, they signify encodings. -1 means that they are
  /// NULL.
  static bool is_null(const Int _val) { return _val < 0; }

  /// Checks whether a float is NaN.
  static bool is_null(const Float _val) { return std::isnan(_val); }

  /// Checks whether a string is on the list of strings interpreted as NULL.
  static bool is_null(const strings::String& _val) { return !_val; }

  /// Checks whether a string is on the list of strings interpreted as NULL.
  static bool is_null(const std::string& _val) {
    return is_null(strings::String::parse_null(_val));
  }

  /// Returns the NULL value for a particular type.
  template <class T>
  static T make_null() {
    if constexpr (std::is_same<T, Int>()) {
      return -1;
    }

    if constexpr (std::is_same<T, Float>()) {
      return NAN;
    }

    if constexpr (std::is_same<T, strings::String>()) {
      return strings::String("NULL");
    }

    static_assert(std::is_same<T, Int>() || std::is_same<T, Float>() ||
                      std::is_same<T, strings::String>(),
                  "Unsupported type");
  }
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // ENGINE_UTILS_NULLCHECKER_HPP_
