// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_
#define ENGINE_COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_

#include <variant>

#include "engine/Float.hpp"
#include "engine/commands/BooleanColumnView.hpp"
#include "engine/commands/StringColumnOrStringColumnView.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace commands {

class BooleanColumnView;
class StringColumnOrStringColumnView;

class FloatColumnOrFloatColumnView {
 public:
  /// The command used for arange operations.
  using FloatArangeOp =
      fct::NamedTuple<fct::Field<"operator_", fct::Literal<"arange">>,
                      fct::Field<"start_", Float>, fct::Field<"stop_", Float>,
                      fct::Field<"step_", Float>,
                      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for transforming string columns to float columns.
  using FloatAsTSOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"as_ts">>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"time_formats_", std::vector<std::string>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The possible operators.
  using FloatBinaryOpLiteral =
      fct::Literal<"divides", "fmod", "minus", "multiplies", "plus", "pow">;

  /// The command used for float binary operations.
  using FloatBinaryOp = fct::NamedTuple<
      fct::Field<"operator_", FloatBinaryOpLiteral>,
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operand2_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for retrieving float columns from a data frame.
  using FloatColumnOp =
      fct::NamedTuple<fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"role_", std::string>,
                      fct::Field<"type_", fct::Literal<"FloatColumn">>>;

  /// The command used for float const operations.
  using FloatConstOp =
      fct::NamedTuple<fct::Field<"operator_", fct::Literal<"const">>,
                      fct::Field<"value_", Float>,
                      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for transforming boolean column views to float columns.
  using FloatFromBooleanOp =
      fct::NamedTuple<fct::Field<"operator_", fct::Literal<"boolean_as_num">>,
                      fct::Field<"operand1_", fct::Ref<BooleanColumnView>>,
                      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for transforming string columns to float columns.
  using FloatFromStringOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"as_num">>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for random operations.
  using FloatRandomOp =
      fct::NamedTuple<fct::Field<"operator_", fct::Literal<"random">>,
                      fct::Field<"seed_", unsigned int>,
                      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for float subselection operations.
  using FloatSubselectionOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"subselection">>,
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operand2_",
                 std::variant<fct::Ref<FloatColumnOrFloatColumnView>,
                              fct::Ref<BooleanColumnView>>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The possible operators.
  using FloatUnaryOpLiteral =
      fct::Literal<"abs", "acos", "asin", "atan", "cbrt", "ceil", "cos", "day",
                   "erf", "exp", "floor", "hour", "lgamma", "log", "minute",
                   "month", "round", "rowid", "second", "sin", "sqrt", "tan",
                   "tgamma", "weekday", "year", "yearday">;

  /// The command used for float unary operations.
  using FloatUnaryOp = fct::NamedTuple<
      fct::Field<"operator_", FloatUnaryOpLiteral>,
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for float binary operations.
  using FloatUpdateOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"update">>,
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operand2_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"condition_", fct::Ref<BooleanColumnView>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for string with subtoles operations.
  using FloatWithSubrolesOp = fct::NamedTuple<
      fct::Field<"subroles_", std::vector<std::string>>,
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for float with unit operations.
  using FloatWithUnitOp = fct::NamedTuple<
      fct::Field<"unit_", std::string>,
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  using RecursiveType =
      std::variant<FloatColumnOp, FloatArangeOp, FloatAsTSOp, FloatBinaryOp,
                   FloatConstOp, FloatFromBooleanOp, FloatFromStringOp,
                   FloatRandomOp, FloatSubselectionOp, FloatUnaryOp,
                   FloatUpdateOp, FloatWithSubrolesOp, FloatWithUnitOp>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  //
