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
  /// The possible operators for a binary operations.
  using StringBinaryOpLiteral = rfl::Literal<"concat">;

  /// The command used for boolean binary operations.
  using StringBinaryOp = rfl::NamedTuple<
      rfl::Field<"operator_", StringBinaryOpLiteral>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used for string const operations.
  using StringConstOp =
      rfl::NamedTuple<rfl::Field<"operator_", rfl::Literal<"const">>,
                      rfl::Field<"value_", std::string>,
                      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used to retrieve a substring.
  using StringSubstringOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"substr">>,
      rfl::Field<"begin_", size_t>, rfl::Field<"len_", size_t>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used for string unary operations.
  using StringUnaryOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"as_str">>,
      rfl::Field<"operand1_",
                 std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                              rfl::Ref<BooleanColumnView>>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used for string with subtoles operations.
  using StringWithSubrolesOp = rfl::NamedTuple<
      rfl::Field<"subroles_", std::vector<std::string>>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used for string with unit operations.
  using StringWithUnitOp = rfl::NamedTuple<
      rfl::Field<"unit_", std::string>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used for boolean subselection operations.
  using StringSubselectionOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"subselection">>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"operand2_",
                 std::variant<rfl::Ref<BooleanColumnView>,
                              rfl::Ref<FloatColumnOrFloatColumnView>>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used to update a string column.
  using StringUpdateOp = rfl::NamedTuple<
      rfl::Field<"operator_", rfl::Literal<"update">>,
      rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>>,
      rfl::Field<"condition_", rfl::Ref<BooleanColumnView>>,
      rfl::Field<"type_", rfl::Literal<"StringColumnView">>>;

  /// The command used for retrieving string columns from a data frame.
  using StringColumnOp =
      rfl::NamedTuple<rfl::Field<"df_name_", std::string>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"type_", rfl::Literal<"StringColumn">>>;

  using NamedTupleType =
      std::variant<StringColumnOp, StringBinaryOp, StringConstOp,
                   StringSubselectionOp, StringSubstringOp, StringUnaryOp,
                   StringUpdateOp, StringWithSubrolesOp, StringWithUnitOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif  //
