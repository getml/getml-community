// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/handlers/AggOpParser.hpp"

#include <string>

// ----------------------------------------------------------------------------

#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/CatOpParser.hpp"
#include "engine/handlers/NumOpParser.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace handlers {

Float AggOpParser::categorical_aggregation(
    const std::string& _type, const Poco::JSON::Object& _json_col) {
  const auto col = CatOpParser(categories_, join_keys_encoding_, data_frames_)
                       .parse(_json_col);

  const auto to_str = [](const strings::String& _str) -> std::string {
    return _str.str();
  };

  auto range = col | VIEWS::transform(to_str);

  if (_type == "count_categorical") {
    return utils::Aggregations::count_categorical(range.begin(), range.end());
  }

  if (_type == "count_distinct") {
    return utils::Aggregations::count_distinct(range.begin(), range.end());
  }

  throw std::runtime_error("Aggregation '" + _type +
                           "' not recognized for a categorical column.");
}

// ----------------------------------------------------------------------------

Float AggOpParser::aggregate(const Poco::JSON::Object& _aggregation) {
  const auto type = JSON::get_value<std::string>(_aggregation, "type_");

  const auto json_col = *JSON::get_object(_aggregation, "col_");

  if (type == "count_categorical" || type == "count_distinct") {
    return categorical_aggregation(type, json_col);
  } else {
    return numerical_aggregation(type, json_col);
  }
}

// ----------------------------------------------------------------------------

Float AggOpParser::numerical_aggregation(const std::string& _type,
                                         const Poco::JSON::Object& _json_col) {
  const auto col = NumOpParser(categories_, join_keys_encoding_, data_frames_)
                       .parse(_json_col);

  if (_type == "assert_equal") {
    return utils::Aggregations::assert_equal(col.begin(), col.end());
  } else if (_type == "avg") {
    return utils::Aggregations::avg(col.begin(), col.end());
  } else if (_type == "count") {
    return utils::Aggregations::count(col.begin(), col.end());
  } else if (_type == "max") {
    return utils::Aggregations::maximum(col.begin(), col.end());
  } else if (_type == "median") {
    return utils::Aggregations::median(col.begin(), col.end());
  } else if (_type == "min") {
    return utils::Aggregations::minimum(col.begin(), col.end());
  } else if (_type == "stddev") {
    return utils::Aggregations::stddev(col.begin(), col.end());
  } else if (_type == "sum") {
    return utils::Aggregations::sum(col.begin(), col.end());
  } else if (_type == "var") {
    return utils::Aggregations::var(col.begin(), col.end());
  } else {
    throw std::runtime_error("Aggregation '" + _type +
                             "' not recognized for a numerical column.");

    return 0.0;
  }
}

}  // namespace handlers
}  // namespace engine
