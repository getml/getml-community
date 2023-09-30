// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_
#define COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_

#include <string>
#include <variant>

#include "commands/BooleanColumnView.hpp"
#include "commands/Float.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace commands {

class BooleanColumnView;
class FloatColumnOrFloatColumnView;

class StringColumnOrStringColumnView {
 public:
  /// The command used for boolean binary operations.
  struct StringBinaryOp {
    using StringBinaryOpLiteral = rfl::Literal<"concat">;

    rfl::Field<"operator_", StringBinaryOpLiteral> op;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>> operand2;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string const operations.
  struct StringConstOp {
    rfl::Field<"operator_", rfl::Literal<"const">> op;
    rfl::Field<"value_", std::string> value;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used to retrieve a substring.
  struct StringSubstringOp {
    rfl::Field<"operator_", rfl::Literal<"substr">> op;
    rfl::Field<"begin_", size_t> begin;
    rfl::Field<"len_", size_t> len;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string unary operations.
  struct StringUnaryOp {
    rfl::Field<"operator_", rfl::Literal<"as_str">> op;
    rfl::Field<"operand1_", std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                                         rfl::Ref<BooleanColumnView>>>
        operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string with subtoles operations.
  struct StringWithSubrolesOp {
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string with unit operations.
  struct StringWithUnitOp {
    rfl::Field<"unit_", std::string> unit;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for boolean subselection operations.
  struct StringSubselectionOp {
    rfl::Field<"operator_", rfl::Literal<"subselection">> op;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_",
               std::variant<rfl::Ref<BooleanColumnView>,
                            rfl::Ref<FloatColumnOrFloatColumnView>>>
        operand2;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used to update a string column.
  struct StringUpdateOp {
    rfl::Field<"operator_", rfl::Literal<"update">> op;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>> operand2;
    rfl::Field<"condition_", rfl::Ref<BooleanColumnView>> condition;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for retrieving string columns from a data frame.
  struct StringColumnOp {
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"type_", rfl::Literal<"StringColumn">> type;
  };

  using ReflectionType =
      std::variant<StringColumnOp, StringBinaryOp, StringConstOp,
                   StringSubselectionOp, StringSubstringOp, StringUnaryOp,
                   StringUpdateOp, StringWithSubrolesOp, StringWithUnitOp>;

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace commands

#endif  //
