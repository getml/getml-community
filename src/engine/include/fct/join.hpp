// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FCT_JOIN_HPP_
#define FCT_JOIN_HPP_

#include <sstream>
#include <vector>

namespace fct {

/// Necessary work-around, as join is not supported on Windows yet.
struct join {
  template <class T, class RangeType>
  static std::vector<T> vector(RangeType _range) {
    std::vector<T> result;

    for (const auto& r : _range) {
      for (const auto& v : r) {
        result.emplace_back(v);
      }
    }

    return result;
  }

  template <class T>
  static std::vector<T> vector(
      const std::initializer_list<std::vector<T>>& _range) {
    std::vector<T> result;

    for (const auto& r : _range) {
      for (const auto& v : r) {
        result.emplace_back(v);
      }
    }

    return result;
  }

  /// Generates a string from a range and inserts a seperator.
  template <class RangeType>
  static std::string string(RangeType range, const std::string& _sep = ", ") {
    std::stringstream stream;

    for (auto it = range.begin(); it != range.end();) {
      stream << *it;
      ++it;
      if (it != range.end()) {
        stream << _sep;
      }
    }

    return stream.str();
  }
};

// -------------------------------------------------------------------------
}  // namespace fct

#endif  // FCT_JOIN_HPP_
