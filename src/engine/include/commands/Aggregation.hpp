// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_AGGREGATION_HPP_
#define COMMANDS_AGGREGATION_HPP_

#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

class Aggregation {
 public:
  /// All possible float aggregations.
  using FloatAggregationLiteral = rfl::Literal<"avg", "count", "max", "median",
                                               "min", "stddev", "sum", "var">;

  /// An aggregation over a float column.
  using FloatAggregationOp =
      rfl::NamedTuple<rfl::Field<"type_", FloatAggregationLiteral>,
                      rfl::Field<"col_", FloatColumnOrFloatColumnView> >;

  /// All possible string aggregations.
  using StringAggregationLiteral =
      rfl::Literal<"count_categorical", "count_distinct">;

  /// An aggregation over a string column.
  using StringAggregationOp =
      rfl::NamedTuple<rfl::Field<"type_", StringAggregationLiteral>,
                      rfl::Field<"col_", StringColumnOrStringColumnView> >;

  using NamedTupleType =
      rfl::TaggedUnion<"type_", FloatAggregationOp, StringAggregationOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif
