#ifndef HELPERS_NULLCHECKER_HPP_
#define HELPERS_NULLCHECKER_HPP_

// ----------------------------------------------------------------------------

#include <cmath>
#include <string>

// ----------------------------------------------------------------------------

#include "strings/String.hpp"

// ----------------------------------------------------------------------------

#include "helpers/Float.hpp"
#include "helpers/Int.hpp"

// ----------------------------------------------------------------------------

namespace helpers {
// ----------------------------------------------------------------------------

struct NullChecker {
  /// When ever we us integers, they signify encodings. -1 means that they are
  /// NULL.
  static bool is_null(const Int _val) { return _val < 0; }

  /// Checks whether a float is NaN.
  static bool is_null(const Float _val) { return std::isnan(_val); }

  /// Checks whether a string is on the list of strings interpreted as NULL.
  static bool is_null(const strings::String& _val) {
    return (_val == "" || _val == "nan" || _val == "NaN" || _val == "NA" ||
            _val == "NULL" || _val == "none" || _val == "None");
  }

  /// Checks whether a string is on the list of strings interpreted as NULL.
  static bool is_null(const std::string& _val) {
    return is_null(strings::String(_val));
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
