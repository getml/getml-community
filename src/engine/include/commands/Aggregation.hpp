// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_AGGREGATION_HPP_
#define COMMANDS_AGGREGATION_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/TaggedUnion.hpp>

#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"

namespace commands {

class Aggregation {
 public:
  /// An aggregation over a float column.
  struct FloatAggregationOp {
    using FloatAggregationLiteral =
        rfl::Literal<"avg", "count", "max", "median", "min", "stddev", "sum",
                     "var">;

    rfl::Field<"type_", FloatAggregationLiteral> type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// An aggregation over a string column.
  struct StringAggregationOp {
    using StringAggregationLiteral =
        rfl::Literal<"count_categorical", "count_distinct">;

    rfl::Field<"type_", StringAggregationLiteral> type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  using ReflectionType = std::variant<FloatAggregationOp, StringAggregationOp>;

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace commands

#endif
