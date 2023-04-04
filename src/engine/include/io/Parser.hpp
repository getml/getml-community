// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef IO_PARSER_HPP_
#define IO_PARSER_HPP_

#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/DateTimeParser.h>
#include <Poco/Timestamp.h>

#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "io/Float.hpp"
#include "io/Int.hpp"

namespace io {

class Parser {
 public:
  /// Removes the quotechars.
  static std::string remove_quotechars(const std::string& _str,
                                       const char _quotechar) {
    auto field = _str;

    field.erase(std::remove(field.begin(), field.end(), _quotechar),
                field.end());

    return field;
  }

  // -------------------------------

  /// Transforms a string to a double.
  static std::pair<Float, bool> to_double(const std::string& _str) {
    const auto trimmed = trim(_str);

    if (trimmed.find_first_not_of("0123456789.e-+") != std::string::npos) {
      if (trimmed == "true" || trimmed == "TRUE" || trimmed == "True") {
        return std::pair<Float, bool>(1.0, true);
      }

      if (trimmed == "false" || trimmed == "FALSE" || trimmed == "False") {
        return std::pair<Float, bool>(0.0, true);
      }

      return std::pair<Float, bool>(0.0, false);
    }

    char* ptr = nullptr;

    const auto val = strtod(trimmed.c_str(), &ptr);

    if (val == 0.0 && ptr == trimmed.c_str()) {
      return std::pair<Float, bool>(0.0, false);
    }

    return std::pair<Float, bool>(val, true);
  }

  // -------------------------------

  /// Transforms a string to an integer.
  static std::pair<Int, bool> to_int(const std::string& _str) {
    try {
      const auto trimmed = trim(_str);

      const auto val = std::stoi(trimmed);

      if (std::to_string(val) != trimmed) {
        if (trimmed == "true" || trimmed == "TRUE" || trimmed == "True") {
          return std::pair<Int, bool>(1, true);
        }

        if (trimmed == "false" || trimmed == "FALSE" || trimmed == "False") {
          return std::pair<Int, bool>(0, true);
        }

        return std::pair<Int, bool>(0, false);
      }

      return std::pair<Int, bool>(val, true);
    } catch (std::exception& e) {
      return std::pair<Int, bool>(0, false);
    }
  }

  // -------------------------------

  /// Custom boolean-to-string conversion
  static std::string to_string(const bool _val) {
    if (_val) {
      return "true";
    }
    return "false";
  }

  // -------------------------------

  /// Custom floating-point-to-string conversion (produces more precise
  /// numbers than std::to_string)
  static std::string to_precise_string(const Float _val) {
    std::stringstream stream;
    stream.precision(16);
    stream << _val;
    return stream.str();
  }

  /// Custom floating-point-to-string conversion (produces more beautiful
  /// numbers than std::to_string)
  static std::string to_string(const Float _val) {
    constexpr unsigned long precision = 4;

    const Float delta = 1.0 / std::pow(10.0, static_cast<Float>(precision));

    const auto is_approx_full = [delta](const Float val) {
      if (val != 0.0 && std::round(val) == 0.0) {
        return false;
      }
      return std::abs(std::fmod(val, 1.0)) <= delta;
    };

    std::ostringstream stream;

    if (is_approx_full(_val)) {
      stream << static_cast<long>(_val);
    } else if (std::abs(_val) > 1.0) {
      stream << format_fixed_without_zeros(_val, precision);
    } else {
      stream << std::setprecision(precision) << _val;
    }

    return stream.str();
  }

  // -------------------------------

  /// Transforms a string to a time stamp.
  static std::pair<Float, bool> to_time_stamp(
      const std::string& _str, const std::vector<std::string>& _time_formats) {
    const auto trimmed = trim(_str);

    int utc = Poco::DateTimeFormatter::UTC;

    for (const auto& fmt : _time_formats) {
      Poco::DateTime date_time;

      const auto success =
          Poco::DateTimeParser::tryParse(fmt, trimmed, date_time, utc);

      const auto time_stamp = date_time.timestamp();

      if (!success ||
          Poco::DateTimeFormatter::format(time_stamp, fmt) != trimmed) {
        continue;
      }

      return std::pair<Float, bool>(
          static_cast<Float>(time_stamp.epochMicroseconds()) / 1.0e6, true);
    }

    return std::pair<Float, bool>(0.0, false);
  }

  // -------------------------------

  /// Removes all whitespaces at the beginning and end of the string.
  static std::string trim(const std::string& _str) {
    const auto pos = _str.find_first_not_of("\t\v\f\r\n ");

    if (pos == std::string::npos) {
      return "";
    }

    const auto len = _str.find_last_not_of("\t\v\f\r\n ") - pos + 1;

    return _str.substr(pos, len);
  }

  // -------------------------------

  /// Represents as time stamp in string format.
  static std::string ts_to_string(const Float& _time_stamp_float) {
    if (std::isnan(_time_stamp_float) || std::isinf(_time_stamp_float)) {
      return "NULL";
    }

    const auto microseconds_since_epoch =
        static_cast<Poco::Timestamp::TimeVal>(1e06 * _time_stamp_float);

    const auto time_stamp = Poco::Timestamp(microseconds_since_epoch);

    if (std::fmod(_time_stamp_float, 86400.0) == 0.0) {
      return Poco::DateTimeFormatter::format(time_stamp, "%Y-%m-%d");
    }

    if (std::floor(_time_stamp_float) == _time_stamp_float) {
      return Poco::DateTimeFormatter::format(
          time_stamp, Poco::DateTimeFormat::SORTABLE_FORMAT);
    }

    return Poco::DateTimeFormatter::format(time_stamp, "%Y-%m-%d %H:%M:%S.%F");
  }

  // -------------------------------

 private:
  /// Uses fixed format, but trims trailing zeros.
  static std::string format_fixed_without_zeros(
      const Float _val, const unsigned long _precision) {
    std::ostringstream stream;

    stream << std::fixed << std::setprecision(_precision) << _val;

    const auto str = stream.str();
    const auto pos = str.find_last_not_of("0") + 1;

    return str.substr(0, pos);
  }
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_PARSER_HPP_
