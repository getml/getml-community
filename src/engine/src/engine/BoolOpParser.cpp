// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/BoolOpParser.hpp"

#include <algorithm>

#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"
#include "fct/visit.hpp"

namespace engine {
namespace handlers {

containers::ColumnView<bool> BoolOpParser::binary_operation(
    const BooleanBinaryOp& _cmd) const {
  const auto handle =
      [this](const auto& _literal,
             const BooleanBinaryOp& _cmd) -> containers::ColumnView<bool> {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, fct::Literal<"and">>()) {
      return bin_op(_cmd, std::logical_and<bool>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"equal_to">>()) {
      return bin_op(_cmd, std::equal_to<bool>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"not_equal_to">>()) {
      return bin_op(_cmd, std::not_equal_to<bool>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"or">>()) {
      return bin_op(_cmd, std::logical_or<bool>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"xor">>()) {
      // logical_xor for boolean is the same thing as not_equal_to.
      return bin_op(_cmd, std::not_equal_to<bool>());
    }
  };

  return fct::visit(handle, _cmd.get<"operator_">(), _cmd);
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::numerical_comparison(
    const BooleanNumComparisonOp& _cmd) const {
  const auto handle =
      [this](
          const auto& _literal,
          const BooleanNumComparisonOp& _cmd) -> containers::ColumnView<bool> {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, fct::Literal<"equal_to">>()) {
      return num_bin_op(_cmd, std::equal_to<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"greater">>()) {
      return num_bin_op(_cmd, std::greater<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"greater_equal">>()) {
      return num_bin_op(_cmd, std::greater_equal<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"less">>()) {
      return num_bin_op(_cmd, std::less<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"less_equal">>()) {
      return num_bin_op(_cmd, std::less_equal<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"not_equal_to">>()) {
      return num_bin_op(_cmd, std::not_equal_to<Float>());
    }
  };

  return fct::visit(handle, _cmd.get<"operator_">(), _cmd);
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::parse(
    const commands::BooleanColumnView& _cmd) const {
  const auto handle = [this](const auto& _cmd) -> containers::ColumnView<bool> {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, BooleanBinaryOp>()) {
      return binary_operation(_cmd);
    }

    if constexpr (std::is_same<Type, BooleanConstOp>()) {
      const auto value = fct::get<"value_">(_cmd);
      return containers::ColumnView<bool>::from_value(value);
    }

    if constexpr (std::is_same<Type, BooleanIsInfOp>()) {
      const auto& col = *fct::get<"operand1_">(_cmd);
      const auto is_inf = [](const Float val) { return std::isinf(val); };
      return num_un_op(col, is_inf);
    }

    if constexpr (std::is_same<Type, BooleanIsNullOp>()) {
      return is_null(_cmd);
    }

    if constexpr (std::is_same<Type, BooleanNotOp>()) {
      const auto col = parse(*fct::get<"operand1_">(_cmd));
      return containers::ColumnView<bool>::from_un_op(col,
                                                      std::logical_not<bool>());
    }

    if constexpr (std::is_same<Type, BooleanNumComparisonOp>()) {
      return numerical_comparison(_cmd);
    }

    if constexpr (std::is_same<Type, BooleanStrComparisonOp>()) {
      return string_comparison(_cmd);
    }

    if constexpr (std::is_same<Type, BooleanSubselectionOp>()) {
      return subselection(_cmd);
    }

    if constexpr (std::is_same<Type, BooleanUpdateOp>()) {
      return update(_cmd);
    }
  };

  return std::visit(handle, _cmd.val_);
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::subselection(
    const BooleanSubselectionOp& _cmd) const {
  const auto handle =
      [this, &_cmd](const auto& _operand2) -> containers::ColumnView<bool> {
    using Type = std::decay_t<decltype(_operand2)>;

    const auto data = parse(*_cmd.get<"operand1_">());

    if constexpr (std::is_same<
                      Type,
                      fct::Ref<commands::FloatColumnOrFloatColumnView>>()) {
      const auto indices =
          FloatOpParser(categories_, join_keys_encoding_, data_frames_)
              .parse(*_operand2);
      return containers::ColumnView<bool>::from_numerical_subselection(data,
                                                                       indices);
    } else {
      const auto indices = parse(*_operand2);
      return containers::ColumnView<bool>::from_boolean_subselection(data,
                                                                     indices);
    }
  };

  return std::visit(handle, _cmd.get<"operand2_">());
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::string_comparison(
    const BooleanStrComparisonOp& _cmd) const {
  const auto handle =
      [this](
          const auto& _literal,
          const BooleanStrComparisonOp& _cmd) -> containers::ColumnView<bool> {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, fct::Literal<"contains">>()) {
      const auto contains = [](const auto& _str1, const auto& _str2) {
        if (!_str1 || !_str2) {
          return false;
        }
        return (_str1.str().find(_str2.c_str()) != std::string::npos);
      };
      return cat_bin_op(_cmd, contains);
    }

    if constexpr (std::is_same<Type, fct::Literal<"equal_to">>()) {
      return cat_bin_op(_cmd, std::equal_to<strings::String>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"not_equal_to">>()) {
      return cat_bin_op(_cmd, std::not_equal_to<strings::String>());
    }
  };

  return fct::visit(handle, _cmd.get<"operator_">(), _cmd);
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::update(
    const BooleanUpdateOp& _cmd) const {
  const auto operand1 = parse(*_cmd.get<"operand1_">());

  const auto operand2 = parse(*_cmd.get<"operand2_">());

  const auto condition = parse(*_cmd.get<"condition_">());

  const auto op = [](const auto& _val1, const auto& _val2,
                     const bool _cond) -> bool {
    return _cond ? _val2 : _val1;
  };

  return containers::ColumnView<bool>::from_tern_op(operand1, operand2,
                                                    condition, op);
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::is_null(
    const BooleanIsNullOp& _cmd) const {
  const auto handle = [this](const auto& _col) -> containers::ColumnView<bool> {
    using Type = std::decay_t<decltype(_col)>;

    if constexpr (std::is_same<
                      Type,
                      fct::Ref<commands::FloatColumnOrFloatColumnView>>()) {
      const auto is_nan = [](const Float val) { return std::isnan(val); };
      return num_un_op(*_col, is_nan);
    }

    if constexpr (std::is_same<
                      Type,
                      fct::Ref<commands::StringColumnOrStringColumnView>>()) {
      const auto is_null = [](const strings::String& _val) { return !_val; };
      return cat_un_op(*_col, is_null);
    }
  };

  return std::visit(handle, _cmd.get<"operand1_">());
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine