// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/CatOpParser.hpp"

#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/NumOpParser.hpp"

namespace engine {
namespace handlers {

containers::ColumnView<strings::String> CatOpParser::binary_operation(
    const StringBinaryOp& _cmd) const {
  const auto concat = [](const strings::String& _val1,
                         const strings::String& _val2) -> strings::String {
    if (!_val1 || !_val2) {
      return strings::String(nullptr);
    }
    return strings::String(_val1.str() + _val2.str());
  };

  return bin_op(_cmd, concat);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::boolean_as_string(
    const Poco::JSON::Object& _col) const {
  const auto obj = *JSON::get_object(_col, "operand1_");

  const auto operand1 =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_).parse(obj);

  const auto to_str = [](const bool val) -> strings::String {
    if (val) {
      return strings::String("true");
    }
    return strings::String("false");
  };

  return containers::ColumnView<strings::String>::from_un_op(operand1, to_str);
}

// ----------------------------------------------------------------------------

void CatOpParser::check(const containers::Column<strings::String>& _col,
                        const std::string& _name,
                        const fct::Ref<const communication::Logger>& _logger,
                        Poco::Net::StreamSocket* _socket) const {
  communication::Warner warner;

  if (_col.size() == 0) {
    warner.send(_socket);
    return;
  }

  const Float length = static_cast<Float>(_col.size());

  const Float num_non_null =
      utils::Aggregations::count_categorical(_col.begin(), _col.end());

  const auto share_null = 1.0 - num_non_null / length;

  if (share_null > 0.9) {
    warner.add(std::to_string(share_null * 100.0) +
               "% of all entries of column '" + _name + "' are NULL values.");
  }

  for (const auto& warning : warner.warnings()) {
    _logger->log("WARNING: " + warning);
  }

  warner.send(_socket);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::get_column(
    const StringColumnOp& _cmd) const {
  const auto name = _cmd.get<"name_">();

  const auto df_name = _cmd.get<"df_name_">();

  const auto it = data_frames_->find(df_name);

  if (it == data_frames_->end()) {
    throw std::runtime_error("Column '" + name + "' is from DataFrame '" +
                             df_name + "', but such a DataFrame is not known.");
  }

  const auto role = it->second.role(name);

  if (role == containers::DataFrame::ROLE_CATEGORICAL) {
    return to_view(it->second.int_column(name, role), categories_);
  }

  if (role == containers::DataFrame::ROLE_JOIN_KEY) {
    return to_view(it->second.int_column(name, role), join_keys_encoding_);
  }

  if (role == containers::DataFrame::ROLE_TEXT) {
    return to_view(it->second.text(name));
  }

  if (role == containers::DataFrame::ROLE_UNUSED ||
      role == containers::DataFrame::ROLE_UNUSED_STRING) {
    return to_view(it->second.unused_string(name));
  }

  throw std::runtime_error(
      "Column '" + name + "' from DataFrame '" + df_name +
      "' is expected to be a StringColumn, but it appears to be a "
      "FloatColumn. You have most likely changed the type when "
      "assigning a new role.");
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::numerical_as_string(
    const Poco::JSON::Object& _col) const {
  const auto obj = *JSON::get_object(_col, "operand1_");

  const auto operand1 =
      NumOpParser(categories_, join_keys_encoding_, data_frames_).parse(obj);

  const auto role = obj.has("role_")
                        ? JSON::get_value<std::string>(obj, "role_")
                        : std::string("");

  if (role == containers::DataFrame::ROLE_TIME_STAMP ||
      operand1.unit().find("time stamp") != std::string::npos) {
    const auto as_str = [](const Float val) -> strings::String {
      if (std::isnan(val) || std::isinf(val)) {
        return strings::String(nullptr);
      }

      const auto microseconds_since_epoch =
          static_cast<Poco::Timestamp::TimeVal>(1.0e06 * val);

      const auto time_stamp = Poco::Timestamp(microseconds_since_epoch);

      return strings::String(Poco::DateTimeFormatter::format(
          time_stamp, Poco::DateTimeFormat::ISO8601_FRAC_FORMAT));
    };

    return containers::ColumnView<strings::String>::from_un_op(operand1,
                                                               as_str);
  }

  const auto as_str = [](const Float val) {
    return io::Parser::to_string(val);
  };

  return containers::ColumnView<strings::String>::from_un_op(operand1, as_str);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::parse(
    const commands::StringColumnOrStringColumnView& _cmd) const {
  const auto handle =
      [this](const auto& _cmd) -> containers::ColumnView<strings::String> {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, StringBinaryOp>()) {
      return binary_operation(_cmd);
    }

    if constexpr (std::is_same<Type, StringColumnOp>()) {
      return get_column(_cmd);
    }

    if constexpr (std::is_same<Type, StringConstOp>()) {
      const auto val = fct::get<"value_">(_cmd);
      return containers::ColumnView<strings::String>::from_value(val);
    }

    if constexpr (std::is_same<Type, StringSubstringOp>()) {
      return substring(_cmd);
    }

    if constexpr (std::is_same<Type, StringWithSubrolesOp>()) {
      return with_subroles(_cmd);
    }

    if constexpr (std::is_same<Type, StringWithUnitOp>()) {
      return with_unit(_cmd);
    }
  };

  return std::visit(handle, _cmd.val_);

  /*

  if (type == STRING_COLUMN_VIEW && op == "with_subroles") {
    return with_subroles(_col);
  }

  if (type == STRING_COLUMN_VIEW && op == "with_unit") {
    return with_unit(_col);
  }

  if (type == STRING_COLUMN_VIEW && op == "subselection") {
    return subselection(_col);
  }

  if (type == STRING_COLUMN_VIEW) {
    if (_col.has("operand2_")) {
      return binary_operation(_col);
    }

    return unary_operation(_col);
  }

  throw std::runtime_error("Column of type '" + type +
                           "' not recognized for categorical columns.");

  return unary_operation(_col);*/
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::subselection(
    const Poco::JSON::Object& _col) const {
  const auto data = parse(*JSON::get_object(_col, "operand1_"));

  const auto indices_json = *JSON::get_object(_col, "operand2_");

  const auto type = JSON::get_value<std::string>(indices_json, "type_");

  if (type == FLOAT_COLUMN || type == FLOAT_COLUMN_VIEW) {
    const auto indices =
        NumOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(indices_json);

    return containers::ColumnView<strings::String>::from_numerical_subselection(
        data, indices);
  }

  const auto indices =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(indices_json);

  return containers::ColumnView<strings::String>::from_boolean_subselection(
      data, indices);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::substring(
    const StringSubstringOp& _cmd) const {
  const auto begin = _cmd.get<"begin_">();
  const auto len = _cmd.get<"len_">();
  const auto operand1 = parse(*_cmd.get<"operand1_">());
  const auto substr = [begin,
                       len](const strings::String& _val) -> strings::String {
    return _val ? strings::String(_val.str().substr(begin, len)) : _val;
  };
  return containers::ColumnView<strings::String>::from_un_op(operand1, substr);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::unary_operation(
    const StringUnaryOp& _cmd) const {
  // TODO

  /*  const auto op = JSON::get_value<std::string>(_col, "operator_");

const auto operand_type = JSON::get_value<std::string>(
    *JSON::get_object(_col, "operand1_"), "type_");

const auto is_boolean = (operand_type == BOOLEAN_COLUMN_VIEW);

if (is_boolean && op == "as_str") {
  return boolean_as_string(_col);
}

if (!is_boolean && op == "as_str") {
  return numerical_as_string(_col);
}*/
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::to_view(
    const containers::Column<Int>& _col,
    const fct::Ref<const containers::Encoding>& _encoding) const {
  const auto to_str = [_encoding,
                       _col](size_t _i) -> std::optional<strings::String> {
    if (_i >= _col.nrows()) {
      return std::nullopt;
    }

    return (*_encoding)[_col[_i]];
  };

  return containers::ColumnView<strings::String>(to_str, _col.nrows(),
                                                 _col.subroles(), _col.unit());
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::to_view(
    const containers::Column<strings::String>& _col) const {
  const auto to_str = [_col](size_t _i) -> std::optional<strings::String> {
    if (_i >= _col.nrows()) {
      return std::nullopt;
    }

    return _col[_i];
  };

  return containers::ColumnView<strings::String>(to_str, _col.nrows(),
                                                 _col.subroles(), _col.unit());
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::update(
    const Poco::JSON::Object& _col) const {
  const auto operand1 = parse(*JSON::get_object(_col, "operand1_"));

  const auto operand2 = parse(*JSON::get_object(_col, "operand2_"));

  const auto condition =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*JSON::get_object(_col, "condition_"));

  const auto op = [](const strings::String& _val1, const strings::String& _val2,
                     const bool _cond) -> strings::String {
    return _cond ? _val2 : _val1;
  };

  return containers::ColumnView<strings::String>::from_tern_op(
      operand1, operand2, condition, op);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::with_subroles(
    const StringWithSubrolesOp& _cmd) const {
  const auto col = parse(*_cmd.get<"operand1_">());
  const auto subroles = _cmd.get<"subroles_">();
  return col.with_subroles(subroles);
}

// ----------------------------------------------------------------------------

containers::ColumnView<strings::String> CatOpParser::with_unit(
    const StringWithUnitOp& _cmd) const {
  const auto col = parse(*_cmd.get<"operand1_">());
  const auto unit = _cmd.get<"unit_">();
  return col.with_unit(unit);
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
