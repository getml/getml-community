// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_BOOLEANBINARYOP_HPP_
#define COMMANDS_BOOLEANBINARYOP_HPP_

#include <variant>

#include "commands/Float.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

class FloatColumnOrFloatColumnView;
class StringColumnOrStringColumnView;

class BooleanColumnView {
 public:
  /// The possible operators.
  using BooleanBinaryOpLiteral =
      rfl::Literal<"and", "equal_to", "not_equal_to", "or", "xor">;

  /// The command used for boolean binary operations.
  using BooleanBinaryOp =
      rfl::NamedTuple<rfl::Field<"operator_", BooleanBinaryOpLiteral>,
                      rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"operand2_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// The possible operators.
  using BooleanConstLiteral = rfl::Literal<"const">;

  /// The command used for boolean binary operations.
  using BooleanConstOp =
      rfl::NamedTuple<rfl::Field<"operator_", BooleanConstLiteral>,
                      rfl::Field<"value_", bool>,
                      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// The command used for boolean binary operations.
  using BooleanNotOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"not">>,
                      rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// Contains comparisons between two numerical columns .
  using BooleanNumComparisonOpLiteral =
      rfl::Literal<"equal_to", "greater", "greater_equal", "less", "less_equal",
                   "not_equal_to">;

  /// Contains comparisons between two numerical columns .
  using BooleanNumComparisonOp = rfl::NamedTuple<
      rfl::Field<"operator_", BooleanNumComparisonOpLiteral>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"operand2_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// Contains comparisons between two string columns .
  using BooleanStrComparisonOpLiteral =
      rfl::Literal<"contains", "equal_to", "not_equal_to">;

  /// Contains comparisons between two numerical columns .
  using BooleanStrComparisonOp = rfl::NamedTuple<
      rfl::Field<"operator_", BooleanStrComparisonOpLiteral>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// The command used for boolean subselection operations.
  using BooleanSubselectionOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"subselection">>,
      rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>>,
      rfl::Field<"operand2_",
                 std::variant<rfl::Ref<BooleanColumnView>,
                              rfl::Ref<FloatColumnOrFloatColumnView>>>,
      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// The command used to check whether a column is infinite.
  using BooleanIsInfOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"is_inf">>,
      rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>>,
      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// The command used to check whether a column is NaN or NULL.
  using BooleanIsNullOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"is_nan", "is_null">>,
      rfl::Field<"operand1_",
                 std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                              rfl::Ref<StringColumnOrStringColumnView>>>,
      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// The command used to update a boolean column.
  using BooleanUpdateOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"update">>,
                      rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"operand2_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"condition_", rfl::Ref<BooleanColumnView>>,
                      rfl::Field<"type_", rfl::Literal<"BooleanColumnView">>>;

  /// Defines a boolean column view.
  using NamedTupleType =
      std::variant<BooleanBinaryOp, BooleanConstOp, BooleanIsInfOp,
                   BooleanIsNullOp, BooleanNotOp, BooleanNumComparisonOp,
                   BooleanStrComparisonOp, BooleanSubselectionOp,
                   BooleanUpdateOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_BOOLEANBINARYOP_HPP_
