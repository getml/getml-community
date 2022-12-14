// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FCT_COLLECT_HPP_
#define FCT_COLLECT_HPP_

// -------------------------------------------------------------------------

#include <Poco/JSON/Array.h>

// -------------------------------------------------------------------------

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// -------------------------------------------------------------------------

#include "fct/ranges.hpp"

// -------------------------------------------------------------------------

namespace fct {

struct collect {
  /// Generates Poco::JSON::Array::Ptr from a range
  template <class RangeType>
  static Poco::JSON::Array::Ptr array(RangeType range) {
    auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

    for (const auto& val : range) {
      arr->add(val);
    }

    return arr;
  }

  /// Generates a map from a range
  template <class KeyType, class ValueType, class RangeType>
  static std::map<KeyType, ValueType> map(RangeType range) {
    return std::map<KeyType, ValueType>(range.begin(), range.end());
  }

  /// Generates a string from a range
  template <class RangeType>
  static std::string string(RangeType range) {
    std::stringstream stream;

    for (const auto& val : range) {
      stream << val;
    }

    return stream.str();
  }

  /// Generates a vector from a range
  template <class T, class RangeType>
  static std::vector<T> vector(RangeType range) {
#ifdef __APPLE__
    constexpr bool use_push_back = true;
#else
    constexpr bool use_push_back = !std::is_default_constructible<T>() ||
                                   !std::is_move_assignable<T>() ||
                                   !RANGES::sized_range<RangeType>;
#endif

    if constexpr (use_push_back) {
      auto vec = std::vector<T>();
      for (const auto val : range) {
        vec.emplace_back(val);
      }
      return vec;
    } else {
      auto vec = std::vector<T>(RANGES::size(range));
      for (size_t i = 0; i < vec.size(); ++i) {
        vec[i] = std::move(range[i]);
      }
      return vec;
    }
  }

  /// Generates a set from a range
  template <class T, class RangeType>
  static std::set<T> set(RangeType range) {
    auto s = std::set<T>();

    for (const T& val : range) {
      s.insert(val);
    }

    return s;
  }
};

// -------------------------------------------------------------------------
}  // namespace fct

#endif  // FCT_COLLECT_HPP_
