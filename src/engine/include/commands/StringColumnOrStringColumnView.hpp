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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace commands {

class BooleanColumnView;
class FloatColumnOrFloatColumnView;

class StringColumnOrStringColumnView {
 public:
  /// The possible operators for a binary operations.
  using StringBinaryOpLiteral = fct::Literal<"concat">;

  /// The command used for boolean binary operations.
  using StringBinaryOp = fct::NamedTuple<
      fct::Field<"operator_", StringBinaryOpLiteral>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operand2_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string const operations.
  using StringConstOp =
      fct::NamedTuple<fct::Field<"operator_", fct::Literal<"const">>,
                      fct::Field<"value_", std::string>,
                      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used to retrieve a substring.
  using StringSubstringOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"substr">>,
      fct::Field<"begin_", size_t>, fct::Field<"len_", size_t>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string unary operations.
  using StringUnaryOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"as_str">>,
      fct::Field<"operand1_",
                 std::variant<fct::Ref<FloatColumnOrFloatColumnView>,
                              fct::Ref<BooleanColumnView>>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string with subtoles operations.
  using StringWithSubrolesOp = fct::NamedTuple<
      fct::Field<"subroles_", std::vector<std::string>>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string with unit operations.
  using StringWithUnitOp = fct::NamedTuple<
      fct::Field<"unit_", std::string>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for boolean subselection operations.
  using StringSubselectionOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"subselection">>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operand2_",
                 std::variant<fct::Ref<BooleanColumnView>,
                              fct::Ref<FloatColumnOrFloatColumnView>>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used to update a string column.
  using StringUpdateOp = fct::NamedTuple<
      fct::Field<"operator_", fct::Literal<"update">>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operand2_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"condition_", fct::Ref<BooleanColumnView>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for retrieving string columns from a data frame.
  using StringColumnOp =
      fct::NamedTuple<fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"type_", fct::Literal<"StringColumn">>>;

  using NamedTupleType =
      std::variant<StringColumnOp, StringBinaryOp, StringConstOp,
                   StringSubselectionOp, StringSubstringOp, StringUnaryOp,
                   StringUpdateOp, StringWithSubrolesOp, StringWithUnitOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif  //
