// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_HANDLERS_AGGOPPARSER_HPP_
#define ENGINE_HANDLERS_AGGOPPARSER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <map>
#include <memory>
#include <string>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace handlers {

class AggOpParser {
 public:
  AggOpParser(
      const fct::Ref<const containers::Encoding>& _categories,
      const fct::Ref<const containers::Encoding>& _join_keys_encoding,
      const fct::Ref<const std::map<std::string, containers::DataFrame>>&
          _data_frames)
      : categories_(_categories),
        data_frames_(_data_frames),
        join_keys_encoding_(_join_keys_encoding) {}

  ~AggOpParser() = default;

 public:
  /// Executes an aggregation.
  Float aggregate(const Poco::JSON::Object& _aggregation);

 private:
  /// Aggregates over a categorical column.
  Float categorical_aggregation(const std::string& _type,
                                const Poco::JSON::Object& _json_col);

  /// Parses a particular numerical aggregation.
  Float numerical_aggregation(const std::string& _type,
                              const Poco::JSON::Object& _json_col);

 private:
  /// Encodes the categories used.
  const fct::Ref<const containers::Encoding> categories_;

  /// The DataFrames this is based on.
  const fct::Ref<const std::map<std::string, containers::DataFrame>>
      data_frames_;

  /// Encodes the join keys used.
  const fct::Ref<const containers::Encoding> join_keys_encoding_;
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_AGGOPPARSER_HPP_
