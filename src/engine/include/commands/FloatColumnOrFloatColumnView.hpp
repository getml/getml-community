// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_
#define COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_

#include <variant>

#include "commands/BooleanColumnView.hpp"
#include "commands/Float.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace commands {

class BooleanColumnView;
class StringColumnOrStringColumnView;

class FloatColumnOrFloatColumnView {
 public:
  /// The command used for arange operations.
  using FloatArangeOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"arange">>,
                      rfl::Field<"start_", Float>, rfl::Field<"stop_", Float>,
                      rfl::Field<"step_", Float>,
                      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for transforming string columns to float columns.
  using FloatAsTSOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"as_ts">>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"time_formats_", std::vector<std::string>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The possible operators.
  using FloatBinaryOpLiteral =
      rfl::Literal<"divides", "fmod", "minus", "multiplies", "plus", "pow">;

  /// The command used for float binary operations.
  using FloatBinaryOp = rfl::NamedTuple<
      rfl::Field<"operator_", FloatBinaryOpLiteral>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"operand2_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for retrieving float columns from a data frame.
  using FloatColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn">>,
                      rfl::Field<"df_name_", std::string>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"role_", std::string>>;

  /// The command used for float const operations.
  using FloatConstOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"const">>,
                      rfl::Field<"value_", Float>,
                      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for transforming boolean column views to float columns.
  using FloatFromBooleanOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"boolean_as_num">>,
                      rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for transforming string columns to float columns.
  using FloatFromStringOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"as_num">>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for random operations.
  using FloatRandomOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"random">>,
                      rfl::Field<"seed_", unsigned int>,
                      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for rowid operations.
  using FloatRowidOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"rowid">>,
                      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for float subselection operations.
  using FloatSubselectionOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"subselection">>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"operand2_",
                 std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                              rfl::Ref<BooleanColumnView>>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The possible operators.
  using FloatUnaryOpLiteral =
      rfl::Literal<"abs", "acos", "asin", "atan", "cbrt", "ceil", "cos", "day",
                   "erf", "exp", "floor", "hour", "lgamma", "log", "minute",
                   "month", "round", "second", "sin", "sqrt", "tan", "tgamma",
                   "weekday", "year", "yearday">;

  /// The command used for float unary operations.
  using FloatUnaryOp = rfl::NamedTuple<
      rfl::Field<"operator_", FloatUnaryOpLiteral>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for float binary operations.
  using FloatUpdateOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"update">>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"operand2_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"condition_", rfl::Ref<BooleanColumnView>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for string with subtoles operations.
  using FloatWithSubrolesOp = rfl::NamedTuple<
      rfl::Field<"subroles_", std::vector<std::string>>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  /// The command used for float with unit operations.
  using FloatWithUnitOp = rfl::NamedTuple<
      rfl::Field<"unit_", std::string>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"type_", rfl::Literal<"FloatColumnView">>>;

  using NamedTupleType =
      std::variant<FloatArangeOp, FloatAsTSOp, FloatBinaryOp, FloatConstOp,
                   FloatFromBooleanOp, FloatFromStringOp, FloatRandomOp,
                   FloatRowidOp, FloatSubselectionOp, FloatUnaryOp,
                   FloatUpdateOp, FloatColumnOp, FloatWithSubrolesOp,
                   FloatWithUnitOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif
