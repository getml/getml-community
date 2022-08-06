// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef DATABASE_GETTER_HPP_
#define DATABASE_GETTER_HPP_

// ----------------------------------------------------------------------------

#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "io/io.hpp"

// ----------------------------------------------------------------------------

#include "database/Float.hpp"
#include "database/Int.hpp"

// ----------------------------------------------------------------------------

namespace database {
// ----------------------------------------------------------------------------

struct Getter {
  /// Returns a double .
  static Float get_double(const std::string& _str) {
    const auto [val, success] = io::Parser::to_double(_str);

    if (!success) {
      return static_cast<Float>(NAN);
    }

    return val;
  }

  /// Returns an intger.
  static Int get_int(const std::string& _str) {
    auto [val, success] = io::Parser::to_int(_str);

    if (!success) {
      Float fval = 0.0;
      std::tie(fval, success) = io::Parser::to_double(_str);
      val = static_cast<Int>(fval);
    }

    if (!success) {
      return 0;
    }

    return static_cast<Int>(val);
  }

  /// Returns a time stamp transformed to the number of days since epoch.
  static Float get_time_stamp(const std::string& _str,
                              const std::vector<std::string>& _time_formats) {
    auto [val, success] = io::Parser::to_time_stamp(_str, _time_formats);

    if (!success) {
      std::tie(val, success) = io::Parser::to_double(std::string(_str));
    }

    if (!success) {
      return static_cast<Float>(NAN);
    }

    return val;
  }

  // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_GETTER_HPP_
