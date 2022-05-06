#include "engine/handlers/BoolOpParser.hpp"

// ----------------------------------------------------------------------------

#include "engine/handlers/CatOpParser.hpp"
#include "engine/handlers/NumOpParser.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace handlers {

containers::ColumnView<bool> BoolOpParser::binary_operation(
    const Poco::JSON::Object& _col) const {
  const auto op = JSON::get_value<std::string>(_col, "operator_");

  const auto operand_type = JSON::get_value<std::string>(
      *JSON::get_object(_col, "operand1_"), "type_");

  const auto operand2_type = JSON::get_value<std::string>(
      *JSON::get_object(_col, "operand2_"), "type_");

  if (helpers::StringReplacer::replace_all(operand_type, "View", "") !=
      helpers::StringReplacer::replace_all(operand2_type, "View", "")) {
    throw std::runtime_error(
        "You are trying to compare two different column types: " +
        operand_type + " vs. " + operand2_type + ".");
  }

  const auto is_boolean = (operand_type == BOOLEAN_COLUMN_VIEW);

  const auto is_categorical =
      (operand_type == STRING_COLUMN) || (operand_type == STRING_COLUMN_VIEW);

  const auto is_numerical =
      (operand_type == FLOAT_COLUMN) || (operand_type == FLOAT_COLUMN_VIEW);

  if (op == "and") {
    return bin_op(_col, std::logical_and<bool>());
  }

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

  if (op == "or") {
    return bin_op(_col, std::logical_or<bool>());
  }

  if (op == "xor") {
    // logical_xor for boolean is the same thing as not_equal_to.
    return bin_op(_col, std::not_equal_to<bool>());
  }

  throw std::runtime_error("Operator '" + op +
                           "' not recognized for boolean columns.");

  return bin_op(_col, std::logical_and<bool>());
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::parse(
    const Poco::JSON::Object& _col) const {
  const auto type = JSON::get_value<std::string>(_col, "type_");

  const auto op = JSON::get_value<std::string>(_col, "operator_");

  if (op == "const") {
    const auto value = JSON::get_value<bool>(_col, "value_");
    return containers::ColumnView<bool>::from_value(value);
  }

  if (type == BOOLEAN_COLUMN_VIEW && op == "subselection") {
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

  return unary_operation(_col);
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

containers::ColumnView<bool> BoolOpParser::unary_operation(
    const Poco::JSON::Object& _col) const {
  const auto op = JSON::get_value<std::string>(_col, "operator_");

  if (op == "is_inf") {
    const auto is_inf = [](const Float val) { return std::isinf(val); };
    return num_un_op(_col, is_inf);
  }

  if (op == "is_nan") {
    const auto is_nan = [](const Float val) { return std::isnan(val); };
    return num_un_op(_col, is_nan);
  }

  if (op == "not") {
    return un_op(_col, std::logical_not<bool>());
  }

  throw std::runtime_error("Operator '" + op +
                           "' not recognized for boolen columns.");

  return un_op(_col, std::logical_not<bool>());
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
