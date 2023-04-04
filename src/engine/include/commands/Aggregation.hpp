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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/TaggedUnion.hpp"

namespace commands {

class Aggregation {
 public:
  /// All possible float aggregations.
  using FloatAggregationLiteral = fct::Literal<"avg", "count", "max", "median",
                                               "min", "stddev", "sum", "var">;

  /// An aggregation over a float column.
  using FloatAggregationOp =
      fct::NamedTuple<fct::Field<"type_", FloatAggregationLiteral>,
                      fct::Field<"col_", FloatColumnOrFloatColumnView> >;

  /// All possible string aggregations.
  using StringAggregationLiteral =
      fct::Literal<"count_categorical", "count_distinct">;

  /// An aggregation over a string column.
  using StringAggregationOp =
      fct::NamedTuple<fct::Field<"type_", StringAggregationLiteral>,
                      fct::Field<"col_", StringColumnOrStringColumnView> >;

  using NamedTupleType =
      fct::TaggedUnion<"type_", FloatAggregationOp, StringAggregationOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif
