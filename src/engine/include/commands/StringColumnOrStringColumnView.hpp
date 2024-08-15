// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_
#define COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/Reader.hpp>
#include <string>
#include <variant>

#include "commands/BooleanColumnView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"

namespace commands {

struct BooleanColumnView;
struct FloatColumnOrFloatColumnView;

struct StringColumnOrStringColumnView {
 public:
  /// The command used to concat two strings.
  struct StringBinaryOp {
    using Tag = rfl::Literal<"concat">;

    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>> operand2;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string const operations.
  struct StringConstOp {
    using Tag = rfl::Literal<"const">;
    rfl::Field<"value_", std::string> value;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used to retrieve a substring.
  struct StringSubstringOp {
    using Tag = rfl::Literal<"substr">;
    rfl::Field<"begin_", size_t> begin;
    rfl::Field<"len_", size_t> len;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string unary operations.
  struct StringUnaryOp {
    using Tag = rfl::Literal<"as_str">;
    rfl::Field<"operand1_", std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                                         rfl::Ref<BooleanColumnView>>>
        operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string with subtoles operations.
  struct StringWithSubrolesOp {
    using Tag = rfl::Literal<"str_with_subroles">;
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for string with unit operations.
  struct StringWithUnitOp {
    using Tag = rfl::Literal<"str_with_unit">;
    rfl::Field<"unit_", std::string> unit;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for boolean subselection operations.
  struct StringSubselectionOp {
    using Tag = rfl::Literal<"str_subselection">;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_",
               std::variant<rfl::Ref<BooleanColumnView>,
                            rfl::Ref<FloatColumnOrFloatColumnView>>>
        operand2;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used to update a string column.
  struct StringUpdateOp {
    using Tag = rfl::Literal<"str_update">;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<StringColumnOrStringColumnView>> operand2;
    rfl::Field<"condition_", rfl::Ref<BooleanColumnView>> condition;
    rfl::Field<"type_", rfl::Literal<"StringColumnView">> type;
  };

  /// The command used for retrieving string columns from a data frame.
  struct StringColumnOp {
    using Tag = rfl::Literal<"StringColumn">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"type_", rfl::Literal<"StringColumn">> type;
  };

  using ReflectionType =
      rfl::TaggedUnion<"operator_", StringColumnOp, StringBinaryOp,
                       StringConstOp, StringSubselectionOp, StringSubstringOp,
                       StringUnaryOp, StringUpdateOp, StringWithSubrolesOp,
                       StringWithUnitOp>;

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static StringColumnOrStringColumnView from_json_obj(const InputVarType& _obj);

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace commands

#endif  //
