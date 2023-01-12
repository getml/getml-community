// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_
#define ENGINE_COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_

#include <string>
#include <variant>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace commands {

class StringColumnOrStringColumnView {
 public:
  /// The possible operators for a binary operations.
  using StringBinaryOpLiteral = fct::Literal<"concat">;

  /// The command used for boolean binary operations.
  using StringBinaryOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operand2_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operator_", StringBinaryOpLiteral>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string const operations.
  using StringConstOp =
      fct::NamedTuple<fct::Field<"value_", std::string>,
                      fct::Field<"operator_", fct::Literal<"const">>,
                      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used to retrieve a substring.
  using StringSubstringOp = fct::NamedTuple<
      fct::Field<"begin_", size_t>, fct::Field<"len_", size_t>,
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operator_", fct::Literal<"substr">>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The possible operators.
  using StringUnaryOpLiteral = fct::Literal<"as_str">;

  /// The command used for string unary operations.
  using StringUnaryOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"operator_", StringUnaryOpLiteral>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string with subtoles operations.
  using StringWithSubrolesOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"subroles_", std::vector<std::string>>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for string with unit operations.
  using StringWithUnitOp = fct::NamedTuple<
      fct::Field<"operand1_", fct::Ref<StringColumnOrStringColumnView>>,
      fct::Field<"unit_", std::string>,
      fct::Field<"type_", fct::Literal<"StringColumnView">>>;

  /// The command used for boolean subselection operations.
  // TODO
  /*using StringSubselectionOp =
      fct::NamedTuple<fct::Field<"operand1_", Column>,
                      fct::Field<"operand2_", Column>,
                      fct::Field<"operator_", fct::Literal<"subselection">>,
                      fct::Field<"type_", fct::Literal<"StringColumnView">>>;*/

  /// The command used for retrieving string columns from a data frame.
  using StringColumnOp =
      fct::NamedTuple<fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"type_", fct::Literal<"StringColumn">>>;

  /// Defines a string column view.
  using StringColumnView =
      std::variant<StringBinaryOp, StringConstOp, /*StringSubselectionOp,*/
                   StringUnaryOp, StringWithSubrolesOp, StringWithUnitOp>;

  using RecursiveType = std::variant<StringColumnOp, StringBinaryOp,
                                     StringConstOp, /*StringSubselectionOp,*/
                                     /*StringUnaryOp,*/ StringSubstringOp,
                                     StringWithSubrolesOp, StringWithUnitOp>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  //
