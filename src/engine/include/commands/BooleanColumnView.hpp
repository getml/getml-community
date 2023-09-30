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
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

class FloatColumnOrFloatColumnView;
class StringColumnOrStringColumnView;

struct BooleanColumnView {
  /// The command used for boolean binary operations.
  struct BooleanBinaryOp {
    using BooleanBinaryOpLiteral =
        rfl::Literal<"and", "equal_to", "not_equal_to", "or", "xor">;

    rfl::Field<"operator_", BooleanBinaryOpLiteral> op;
    rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<BooleanColumnView>> operand2;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// The command used for boolean binary operations.
  struct BooleanConstOp {
    using BooleanConstLiteral = rfl::Literal<"const">;

    rfl::Field<"operator_", BooleanConstLiteral> op;
    rfl::Field<"value_", bool> value;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// The command used for boolean binary operations.
  struct BooleanNotOp {
    rfl::Field<"operator_", rfl::Literal<"not">> op;
    rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// Contains comparisons between two numerical columns .
  struct BooleanNumComparisonOp {
    using BooleanNumComparisonOpLiteral =
        rfl::Literal<"equal_to", "greater", "greater_equal", "less",
                     "less_equal", "not_equal_to">;

    rfl::Field<"operator_", BooleanNumComparisonOpLiteral> op;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<FloatColumnOrFloatColumnView>> operand2;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// Contains comparisons between two numerical columns .
  struct BooleanStrComparisonOp {
    using BooleanStrComparisonOpLiteral =
        rfl::Literal<"contains", "equal_to", "not_equal_to">;

    rfl::Field<"operator_", BooleanStrComparisonOpLiteral> op;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>> operand2;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// The command used for boolean subselection operations.
  struct BooleanSubselectionOp {
    rfl::Field<"operator_", rfl::Literal<"subselection">> op;
    rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>> operand1;
    rfl::Field<"operand2_",
               std::variant<rfl::Ref<BooleanColumnView>,
                            rfl::Ref<FloatColumnOrFloatColumnView>>>
        operand2;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// The command used to check whether a column is infinite.
  struct BooleanIsInfOp {
    rfl::Field<"operator_", rfl::Literal<"is_inf">> op;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// The command used to check whether a column is NaN or NULL.
  struct BooleanIsNullOp {
    rfl::Field<"operator_", rfl::Literal<"is_nan", "is_null">> op;
    rfl::Field<"operand1_",
               std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                            rfl::Ref<StringColumnOrStringColumnView>>>
        operand1;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// The command used to update a boolean column.
  struct BooleanUpdateOp {
    rfl::Field<"operator_", rfl::Literal<"update">> op;
    rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<BooleanColumnView>> operand2;
    rfl::Field<"condition_", rfl::Ref<BooleanColumnView>> condition;
    rfl::Field<"type_", rfl::Literal<"BooleanColumnView">> type;
  };

  /// Defines a boolean column view.
  using ReflectionType =
      std::variant<BooleanBinaryOp, BooleanConstOp, BooleanIsInfOp,
                   BooleanIsNullOp, BooleanNotOp, BooleanNumComparisonOp,
                   BooleanStrComparisonOp, BooleanSubselectionOp,
                   BooleanUpdateOp>;

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_BOOLEANBINARYOP_HPP_
