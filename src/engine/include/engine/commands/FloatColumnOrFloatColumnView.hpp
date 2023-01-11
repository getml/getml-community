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
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace commands {

class FloatColumnOrFloatColumnView {
 public:
  /// The command used for arange operations.
  using FloatArangeOp =
      fct::NamedTuple<fct::Field<"start_", Float>, fct::Field<"stop_", Float>,
                      fct::Field<"step_", Float>,
                      fct::Field<"operator_", fct::Literal<"arange">>,
                      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The possible operators.
  using FloatBinaryOpLiteral =
      fct::Literal<"divides", "fmod", "minus", "multiplies", "plus", "pow",
                   "update">;

  /// The command used for boolean binary operations.
  using FloatBinaryOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operand2_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operator_", FloatBinaryOpLiteral>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for retrieving float columns from a data frame.
  using FloatColumn =
      fct::NamedTuple<fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"type_", fct::Literal<"FloatColumn">>>;

  /// The command used for float const operations.
  using FloatConstOp =
      fct::NamedTuple<fct::Field<"value_", std::string>,
                      fct::Field<"operator_", fct::Literal<"const">>,
                      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for float subselection operations.
  using FloatSubselectionOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operand2_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operator_", fct::Literal<"subselection">>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The possible operators.
  using FloatUnaryOpLiteral =
      fct::Literal<"abs", "acos", "as_num", "as_ts", "asin", "atan",
                   "boolean_as_num", "cbrt", "ceil", "cos", "day", "erf", "exp",
                   "floor", "hour", "lgamma", "log", "minute", "month",
                   "random", "round", "rowid", "second", "sin", "sqrt", "tan",
                   "tgamma", "value", "weekday", "year", "yearday">;

  /// The command used for float unary operations.
  using FloatUnaryOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operator_", FloatUnaryOpLiteral>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for string with subtoles operations.
  using FloatWithSubrolesOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"subroles_", std::vector<std::string>>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// The command used for float with unit operations.
  using FloatWithUnitOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"unit_", std::string>,
      fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

  /// Combines the definitions of all different possible FloatColumnViews.
  using FloatColumnView =
      std::variant<FloatArangeOp, FloatBinaryOp, FloatConstOp,
                   FloatSubselectionOp, FloatUnaryOp, FloatWithSubrolesOp,
                   FloatWithUnitOp>;

  using RecursiveType = std::variant<FloatColumn, FloatColumnView>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  //
