// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_AGGOPPARSER_HPP_
#define ENGINE_HANDLERS_AGGOPPARSER_HPP_

#include <map>
#include <string>

#include "commands/Aggregation.hpp"
#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "engine/Float.hpp"

namespace engine {
namespace handlers {

class AggOpParser {
  using FloatAggregationOp = typename commands::Aggregation::FloatAggregationOp;
  using StringAggregationOp =
      typename commands::Aggregation::StringAggregationOp;

 public:
  AggOpParser(
      const rfl::Ref<const containers::Encoding>& _categories,
      const rfl::Ref<const containers::Encoding>& _join_keys_encoding,
      const rfl::Ref<const std::map<std::string, containers::DataFrame>>&
          _data_frames)
      : categories_(_categories),
        data_frames_(_data_frames),
        join_keys_encoding_(_join_keys_encoding) {}

  ~AggOpParser() = default;

 public:
  /// Executes an aggregation.
  Float aggregate(const commands::Aggregation& _aggregation) const;

 private:
  /// Aggregations over a float column.
  Float float_aggregation(const FloatAggregationOp& _cmd) const;

  /// Aggregates over a string column.
  Float string_aggregation(const StringAggregationOp& _cmd) const;

 private:
  /// Encodes the categories used.
  const rfl::Ref<const containers::Encoding> categories_;

  /// The DataFrames this is based on.
  const rfl::Ref<const std::map<std::string, containers::DataFrame>>
      data_frames_;

  /// Encodes the join keys used.
  const rfl::Ref<const containers::Encoding> join_keys_encoding_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_AGGOPPARSER_HPP_
