// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_
#define COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_

#include <variant>

#include "commands/BooleanColumnView.hpp"
#include "commands/Float.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace commands {

class BooleanColumnView;
class StringColumnOrStringColumnView;

struct FloatColumnOrFloatColumnView {
  /// The command used for arange operations.
  struct FloatArangeOp {
    rfl::Field<"operator_", rfl::Literal<"arange">> op;
    rfl::Field<"start_", Float> start;
    rfl::Field<"stop_", Float> stop;
    rfl::Field<"step_", Float> step;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for transforming string columns to float columns.
  struct FloatAsTSOp {
    rfl::Field<"operator_", rfl::Literal<"as_ts">> op;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    rfl::Field<"time_formats_", std::vector<std::string>> time_formats;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for float binary operations.
  struct FloatBinaryOp {
    using FloatBinaryOpLiteral =
        rfl::Literal<"divides", "fmod", "minus", "multiplies", "plus", "pow">;

    rfl::Field<"operator_", FloatBinaryOpLiteral> op;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<FloatColumnOrFloatColumnView>> operand2;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for retrieving float columns from a data frame.
  struct FloatColumnOp {
    using Tag = rfl::Literal<"FloatColumn">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
  };

  /// The command used for float const operations.
  struct FloatConstOp {
    rfl::Field<"operator_", rfl::Literal<"const">> op;
    rfl::Field<"value_", Float> value;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for transforming boolean column views to float columns.
  struct FloatFromBooleanOp {
    rfl::Field<"operator_", rfl::Literal<"boolean_as_num">> op;
    rfl::Field<"operand1_", rfl::Ref<BooleanColumnView>> operand1;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for transforming string columns to float columns.
  struct FloatFromStringOp {
    rfl::Field<"operator_", rfl::Literal<"as_num">> op;
    rfl::Field<"operand1_", rfl::Ref<StringColumnOrStringColumnView>> operand1;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for random operations.
  struct FloatRandomOp {
    rfl::Field<"operator_", rfl::Literal<"random">> op;
    rfl::Field<"seed_", unsigned int> seed;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for rowid operations.
  struct FloatRowidOp {
    rfl::Field<"operator_", rfl::Literal<"rowid">> rowid;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for float subselection operations.
  struct FloatSubselectionOp {
    rfl::Field<"operator_", rfl::Literal<"subselection">> op;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    rfl::Field<"operand2_", std::variant<rfl::Ref<FloatColumnOrFloatColumnView>,
                                         rfl::Ref<BooleanColumnView>>>
        operand2;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for float unary operations.
  struct FloatUnaryOp {
    using FloatUnaryOpLiteral =
        rfl::Literal<"abs", "acos", "asin", "atan", "cbrt", "ceil", "cos",
                     "day", "erf", "exp", "floor", "hour", "lgamma", "log",
                     "minute", "month", "round", "second", "sin", "sqrt", "tan",
                     "tgamma", "weekday", "year", "yearday">;

    rfl::Field<"operator_", FloatUnaryOpLiteral> op;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for float binary operations.
  struct FloatUpdateOp {
    rfl::Field<"operator_", rfl::Literal<"update">> op;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    rfl::Field<"operand2_", rfl::Ref<FloatColumnOrFloatColumnView>> operand2;
    rfl::Field<"condition_", rfl::Ref<BooleanColumnView>> condition;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for string with subtoles operations.
  struct FloatWithSubrolesOp {
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  /// The command used for float with unit operations.
  struct FloatWithUnitOp {
    rfl::Field<"unit_", std::string> unit;
    rfl::Field<"operand1_", rfl::Ref<FloatColumnOrFloatColumnView>> operand1;
    using Tag = rfl::Literal<"FloatColumnView">;
  };

  using ReflectionType =
      std::variant<FloatArangeOp, FloatAsTSOp, FloatBinaryOp, FloatConstOp,
                   FloatFromBooleanOp, FloatFromStringOp, FloatRandomOp,
                   FloatRowidOp, FloatSubselectionOp, FloatUnaryOp,
                   FloatUpdateOp, FloatColumnOp, FloatWithSubrolesOp,
                   FloatWithUnitOp>;

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace commands

#endif
