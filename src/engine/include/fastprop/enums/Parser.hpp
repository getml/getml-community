// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FASTPROP_ENUMS_PARSER_HPP_
#define FASTPROP_ENUMS_PARSER_HPP_

// ----------------------------------------------------------------------------

#include "fastprop/enums/Aggregation.hpp"
#include "fastprop/enums/DataUsed.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace enums {
// ----------------------------------------------------------------------------

template <class T>
class Parser {};

// ----------------------------------------------------------------------------

template <>
struct Parser<Aggregation> : public helpers::enums::Parser<Aggregation> {};

// ----------------------------------------------------------------------------

template <>
struct Parser<DataUsed> {
  static constexpr const char* CATEGORICAL = "categorical";
  static constexpr const char* DISCRETE = "discrete";
  static constexpr const char* LAG = "lag";
  static constexpr const char* NOT_APPLICABLE = "na";
  static constexpr const char* NUMERICAL = "numerical";
  static constexpr const char* SAME_UNITS_CATEGORICAL =
      "same_units_categorical";
  static constexpr const char* SAME_UNITS_DISCRETE = "same_units_discrete";
  static constexpr const char* SAME_UNITS_DISCRETE_TS =
      "same_units_discrete_ts";
  static constexpr const char* SAME_UNITS_NUMERICAL = "same_units_numerical";
  static constexpr const char* SAME_UNITS_NUMERICAL_TS =
      "same_units_numerical_ts";
  static constexpr const char* SUBFEATURES = "subfeatures";
  static constexpr const char* TEXT = "text";

  /// Parse parses a _str.
  static DataUsed parse(const std::string& _str) {
    if (_str == CATEGORICAL) {
      return DataUsed::categorical;
    }

    if (_str == DISCRETE) {
      return DataUsed::discrete;
    }

    if (_str == LAG) {
      return DataUsed::lag;
    }

    if (_str == NOT_APPLICABLE) {
      return DataUsed::not_applicable;
    }

    if (_str == NUMERICAL) {
      return DataUsed::numerical;
    }

    if (_str == SAME_UNITS_CATEGORICAL) {
      return DataUsed::same_units_categorical;
    }

    if (_str == SAME_UNITS_DISCRETE) {
      return DataUsed::same_units_discrete;
    }

    if (_str == SAME_UNITS_DISCRETE_TS) {
      return DataUsed::same_units_discrete_ts;
    }

    if (_str == SAME_UNITS_NUMERICAL) {
      return DataUsed::same_units_numerical;
    }

    if (_str == SAME_UNITS_NUMERICAL_TS) {
      return DataUsed::same_units_numerical_ts;
    }

    if (_str == SUBFEATURES) {
      return DataUsed::subfeatures;
    }

    if (_str == TEXT) {
      return DataUsed::text;
    }

    throw_unless(false, "FastProp: Unknown data used: '" + _str + "'");

    return DataUsed::same_units_numerical;
  }

  /// to_str expresses the aggregation as a string.
  static std::string to_str(const DataUsed _data_used) {
    switch (_data_used) {
      case DataUsed::categorical:
        return CATEGORICAL;

      case DataUsed::discrete:
        return DISCRETE;

      case DataUsed::lag:
        return LAG;

      case DataUsed::not_applicable:
        return NOT_APPLICABLE;

      case DataUsed::numerical:
        return NUMERICAL;

      case DataUsed::same_units_categorical:
        return SAME_UNITS_CATEGORICAL;

      case DataUsed::same_units_discrete:
        return SAME_UNITS_DISCRETE;

      case DataUsed::same_units_discrete_ts:
        return SAME_UNITS_DISCRETE_TS;

      case DataUsed::same_units_numerical:
        return SAME_UNITS_NUMERICAL;

      case DataUsed::same_units_numerical_ts:
        return SAME_UNITS_NUMERICAL_TS;

      case DataUsed::subfeatures:
        return SUBFEATURES;

      case DataUsed::text:
        return TEXT;

      default:
        throw_unless(false, "FastProp: Unknown data used.");
        return "";
    }
  }
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_ENUMS_PARSER_HPP_
