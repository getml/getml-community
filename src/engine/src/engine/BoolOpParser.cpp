// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/BoolOpParser.hpp"

#include <algorithm>

#include "engine/handlers/CatOpParser.hpp"
#include "engine/handlers/NumOpParser.hpp"

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

    if constexpr (std::is_same<Type, fct::Literal<"or">>()) {
      return bin_op(_cmd, std::logical_or<bool>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"xor">>()) {
      // logical_xor for boolean is the same thing as not_equal_to.
      return bin_op(_cmd, std::not_equal_to<bool>());
    }
  };

  return fct::visit(handle, _cmd.get<"operator_">(), _cmd);

  /*
  if (op == "contains") {
    const auto contains = [](const strings::String& _str1,
                             const strings::String& _str2) {
      if (!_str1 || !_str2) {
        return false;
      }
      return (_str1.str().find(_str2.c_str()) != std::string::npos);
    };

    return cat_bin_op(_col, contains);
  }

  if (is_boolean && op == "equal_to") {
    return bin_op(_col, std::equal_to<bool>());
  }

  if (is_categorical && op == "equal_to") {
    return cat_bin_op(_col, std::equal_to<strings::String>());
  }

  if (is_numerical && op == "equal_to") {
    return num_bin_op(_col, std::equal_to<Float>());
  }

  if (op == "greater") {
    return num_bin_op(_col, std::greater<Float>());
  }

  if (op == "greater_equal") {
    return num_bin_op(_col, std::greater_equal<Float>());
  }

  if (op == "less") {
    return num_bin_op(_col, std::less<Float>());
  }

  if (op == "less_equal") {
    return num_bin_op(_col, std::less_equal<Float>());
  }

  if (is_boolean && op == "not_equal_to") {
    return bin_op(_col, std::not_equal_to<bool>());
  }

  if (is_categorical && op == "not_equal_to") {
    return cat_bin_op(_col, std::not_equal_to<strings::String>());
  }

  if (is_numerical && op == "not_equal_to") {
    return num_bin_op(_col, std::not_equal_to<Float>());
  }
*/
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
  };

  return std::visit(handle, _cmd.val_);

  /*  if (type == BOOLEAN_COLUMN_VIEW && op == "subselection") {
      return subselection(_col);
    }

    if (type == BOOLEAN_COLUMN_VIEW) {
      if (_col.has("operand2_")) {
        return binary_operation(_col);
      } else {
        return unary_operation(_col);
      }
    }

    throw std::runtime_error("Column of type '" + type +
                             "' not recognized for boolean columns.");

    return unary_operation(_col);*/
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::subselection(
    const Poco::JSON::Object& _col) const {
  const auto data = parse(*JSON::get_object(_col, "operand1_"));

  const auto indices_json = *JSON::get_object(_col, "operand2_");

  const auto type = JSON::get_value<std::string>(indices_json, "type_");

  if (type == FLOAT_COLUMN || type == FLOAT_COLUMN_VIEW) {
    const auto indices =
        NumOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(indices_json);

    return containers::ColumnView<bool>::from_numerical_subselection(data,
                                                                     indices);
  }

  const auto indices = parse(indices_json);

  return containers::ColumnView<bool>::from_boolean_subselection(data, indices);
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
