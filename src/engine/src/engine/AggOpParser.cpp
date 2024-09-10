// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/AggOpParser.hpp"

#include <rfl/visit.hpp>
#include <string>

#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"
#include "engine/utils/Aggregations.hpp"

namespace engine {
namespace handlers {

Float AggOpParser::aggregate(const commands::Aggregation& _aggregation) const {
  const auto handle = [this](const auto& _cmd) -> Float {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, FloatAggregationOp>()) {
      return float_aggregation(_cmd);
    }

    if constexpr (std::is_same<Type, StringAggregationOp>()) {
      return string_aggregation(_cmd);
    }
  };

  return std::visit(handle, _aggregation.val_);
}

// ----------------------------------------------------------------------------

Float AggOpParser::float_aggregation(const FloatAggregationOp& _cmd) const {
  const auto col = FloatOpParser(categories_, join_keys_encoding_, data_frames_)
                       .parse(_cmd.col());

  const auto handle = [&col](const auto& _literal) -> Float {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, rfl::Literal<"assert_equal">>()) {
      return utils::Aggregations::assert_equal(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"avg">>()) {
      return utils::Aggregations::avg(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"count">>()) {
      return utils::Aggregations::count(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"max">>()) {
      return utils::Aggregations::maximum(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"median">>()) {
      return utils::Aggregations::median(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"min">>()) {
      return utils::Aggregations::minimum(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"stddev">>()) {
      return utils::Aggregations::stddev(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"sum">>()) {
      return utils::Aggregations::sum(col.begin(), col.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"var">>()) {
      return utils::Aggregations::var(col.begin(), col.end());
    }
  };

  return rfl::visit(handle, _cmd.type());
}

// ----------------------------------------------------------------------------

Float AggOpParser::string_aggregation(const StringAggregationOp& _cmd) const {
  const auto col =
      StringOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(_cmd.col());

  const auto to_str = [](const strings::String& _str) -> std::string {
    return _str.str();
  };

  auto range = col | std::views::transform(to_str);

  const auto handle = [&range](const auto& _literal) -> Float {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, rfl::Literal<"count_categorical">>()) {
      return utils::Aggregations::count_categorical(range.begin(), range.end());
    }

    if constexpr (std::is_same<Type, rfl::Literal<"count_distinct">>()) {
      return utils::Aggregations::count_distinct(range.begin(), range.end());
    }
  };

  return rfl::visit(handle, _cmd.type());
}

// ----------------------------------------------------------------------------

}  // namespace handlers
}  // namespace engine
