// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_
#define ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_

#include <variant>

#include "engine/Float.hpp"
#include "engine/commands/FloatColumnOrFloatColumnView.hpp"
#include "engine/commands/StringColumnOrStringColumnView.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace commands {

class FloatColumnOrFloatColumnView;
class StringColumnOrStringColumnView;

class BooleanColumnView {
 public:
  /// The possible operators.
  using BooleanBinaryOpLiteral =
      fct::Literal<"and", /*"contains", "equal_to", "greater", "greater_equal",
                   "less", "less_equal", "not_equal_to",*/
                   "or", "xor">;

  /// The command used for boolean binary operations.
  using BooleanBinaryOp =
      fct::NamedTuple<fct::Field<"operand1_", fct::Ref<BooleanColumnView>>,
                      fct::Field<"operand2_", fct::Ref<BooleanColumnView>>,
                      fct::Field<"operator_", BooleanBinaryOpLiteral>,
                      fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

  /// The possible operators.
  using BooleanConstLiteral = fct::Literal<"const">;

  /// The command used for boolean binary operations.
  using BooleanConstOp =
      fct::NamedTuple<fct::Field<"value_", bool>,
                      fct::Field<"operator_", BooleanConstLiteral>,
                      fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

  /// The command used for boolean binary operations.
  using BooleanNotOp =
      fct::NamedTuple<fct::Field<"operand1_", fct::Ref<BooleanColumnView>>,
                      fct::Field<"operator_", fct::Literal<"not">>,
                      fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

  /// The command used for boolean subselection operations.
  using BooleanSubselectionOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<BooleanColumnView>>,
      fct::Field<"operand2_",
                 std::variant<fct::Ref<BooleanColumnView>,
                              fct::Ref<FloatColumnOrFloatColumnView>>>,
      fct::Field<"operator_", fct::Literal<"subselection">>,
      fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;
  /// The command used to check whether a column is infinite.
  using BooleanIsInfOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<FloatColumnOrFloatColumnView>>,
      fct::Field<"operator_", fct::Literal<"is_inf">>,
      fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

  /// The command used to check whether a column is NaN or NULL.
  using BooleanIsNullOp = fct::NamedTuple<
      fct::Field<"operand1_",
                 std::variant<fct::Ref<FloatColumnOrFloatColumnView>,
                              fct::Ref<StringColumnOrStringColumnView>>>,
      fct::Field<"operator_", fct::Literal<"is_nan", "is_null">>,
      fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

  /// Defines a boolean column view.
  using RecursiveType = std::variant<BooleanBinaryOp, BooleanConstOp, BooleanIsInfOp,BooleanIsNullOp, BooleanNotOp/*,
                                       BooleanSubselectionOp, BooleanUnaryOp*/>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_
