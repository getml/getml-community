#include "engine/handlers/NumOpParser.hpp"

// ----------------------------------------------------------------------------

#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/CatOpParser.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace handlers {
// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::arange(
    const Poco::JSON::Object& _col) const {
  const auto start = JSON::get_value<Float>(_col, "start_");

  const auto stop = JSON::get_value<Float>(_col, "stop_");

  const auto step = JSON::get_value<Float>(_col, "step_");

  const auto value_func = [start, stop,
                           step](const size_t _i) -> std::optional<Float> {
    if (start == stop) {
      return std::nullopt;
    }

    const auto result = start + step * static_cast<Float>(_i);

    const auto end_is_reached =
        (stop > start && result >= stop) || (stop < start && result <= stop);

    if (end_is_reached) {
      return std::nullopt;
    }

    return result;
  };

  if (step == 0.0) {
    throw std::invalid_argument("arange: step cannot be zero.");
  }

  if ((stop - start) * step < 0.0) {
    throw std::invalid_argument(
        "arange: stop - start must have the same sign as step.");
  }

  const auto nrows = std::fmod(stop - start, step) == 0.0
                         ? static_cast<size_t>((stop - start) / step)
                         : static_cast<size_t>((stop - start) / step) + 1;

  return containers::ColumnView<Float>(value_func, nrows);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::as_num(
    const Poco::JSON::Object& _col) const {
  const auto operand1 =
      CatOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*JSON::get_object(_col, "operand1_"));

  const auto to_double = [](const std::string& _str) {
    const auto [val, success] = io::Parser::to_double(_str);
    if (success) {
      return val;
    } else {
      return static_cast<Float>(NAN);
    }
  };

  return containers::ColumnView<Float>::from_un_op(operand1, to_double);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::as_ts(
    const Poco::JSON::Object& _col) const {
  const auto time_formats = JSON::array_to_vector<std::string>(
      JSON::get_array(_col, "time_formats_"));

  const auto operand1 =
      CatOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*JSON::get_object(_col, "operand1_"));

  const auto to_time_stamp = [time_formats](const std::string& _str) {
    auto [val, success] = io::Parser::to_time_stamp(_str, time_formats);

    if (success) {
      return val;
    }

    std::tie(val, success) = io::Parser::to_double(_str);

    if (success) {
      return val;
    }

    return static_cast<Float>(NAN);
  };

  return containers::ColumnView<Float>::from_un_op(operand1, to_time_stamp);
}

// ----------------------------------------------------------------------------

void NumOpParser::check(
    const containers::Column<Float>& _col,
    const std::shared_ptr<const communication::Logger>& _logger,
    Poco::Net::StreamSocket* _socket) const {
  // --------------------------------------------------------------------------

  communication::Warner warner;

  // --------------------------------------------------------------------------

  if (_col.size() == 0) {
    warner.send(_socket);
    return;
  }

  // --------------------------------------------------------------------------

  const Float length = static_cast<Float>(_col.size());

  const Float num_non_null =
      utils::Aggregations::count(_col.begin(), _col.end());

  const auto share_null = 1.0 - num_non_null / length;

  if (share_null > 0.9) {
    warner.add(std::to_string(share_null * 100.0) +
               "% of all entries of column '" + _col.name() +
               "' are NULL values.");
  }

  // --------------------------------------------------------------------------

  if (_logger) {
    for (const auto& warning : warner.warnings()) {
      _logger->log("WARNING: " + warning);
    }
  }

  // --------------------------------------------------------------------------

  warner.send(_socket);

  // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::binary_operation(
    const Poco::JSON::Object& _col) const {
  const auto op = JSON::get_value<std::string>(_col, "operator_");

  if (op == "divides") {
    return bin_op(_col, std::divides<Float>());
  }

  if (op == "fmod") {
    const auto fmod = [](const Float val1, const Float val2) {
      return std::fmod(val1, val2);
    };
    return bin_op(_col, fmod);
  }

  if (op == "minus") {
    return bin_op(_col, std::minus<Float>());
  }

  if (op == "multiplies") {
    return bin_op(_col, std::multiplies<Float>());
  }

  if (op == "plus") {
    return bin_op(_col, std::plus<Float>());
  }

  if (op == "pow") {
    const auto pow = [](const Float val1, const Float val2) {
      return std::pow(val1, val2);
    };
    return bin_op(_col, pow);
  }

  if (op == "update") {
    return update(_col);
  }

  throw std::invalid_argument("Operator '" + op + "' not recognized.");

  return update(_col);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::boolean_as_num(
    const Poco::JSON::Object& _col) const {
  const auto obj = *JSON::get_object(_col, "operand1_");

  const auto operand1 =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_).parse(obj);

  const auto as_num = [](const bool val) {
    if (val) {
      return 1.0;
    } else {
      return 0.0;
    }
  };

  return containers::ColumnView<Float>::from_un_op(operand1, as_num);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::get_column(
    const Poco::JSON::Object& _col) const {
  const auto name = JSON::get_value<std::string>(_col, "name_");

  const auto df_name = JSON::get_value<std::string>(_col, "df_name_");

  const auto it = data_frames_->find(df_name);

  if (it == data_frames_->end()) {
    throw std::invalid_argument("Column '" + name + "' is from DataFrame '" +
                                df_name + "', but no such DataFrame exists.");
  }

  const auto role = it->second.role(name);

  if (role != containers::DataFrame::ROLE_NUMERICAL &&
      role != containers::DataFrame::ROLE_TARGET &&
      role != containers::DataFrame::ROLE_UNUSED_FLOAT &&
      role != containers::DataFrame::ROLE_TIME_STAMP) {
    throw std::invalid_argument(
        "Column '" + name + "' from DataFrame '" + df_name +
        "' is expected to be a FloatColumn, but it appears to be a "
        "StringColumn. You have most likely changed the type when "
        "assigning a new role.");
  }

  const auto float_col = it->second.float_column(name, role);

  return containers::ColumnView<Float>::from_column(float_col);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::parse(
    const Poco::JSON::Object& _col) const {
  const auto type = JSON::get_value<std::string>(_col, "type_");

  if (type == FLOAT_COLUMN) {
    return get_column(_col);
  }

  const auto op = JSON::get_value<std::string>(_col, "operator_");

  if (op == "const") {
    const auto val = JSON::get_value<Float>(_col, "value_");
    return containers::ColumnView<Float>::from_value(val);
  }

  if (type == FLOAT_COLUMN_VIEW && op == "arange") {
    return arange(_col);
  }

  if (type == FLOAT_COLUMN_VIEW && op == "with_subroles") {
    return with_subroles(_col);
  }

  if (type == FLOAT_COLUMN_VIEW && op == "with_unit") {
    return with_unit(_col);
  }

  if (type == FLOAT_COLUMN_VIEW && op == "subselection") {
    return subselection(_col);
  }

  if (type == FLOAT_COLUMN_VIEW && _col.has("operand2_")) {
    return binary_operation(_col);
  }

  if (type == FLOAT_COLUMN_VIEW && !_col.has("operand2_")) {
    return unary_operation(_col);
  }

  throw std::invalid_argument("Column of type '" + type +
                              "' not recognized for numerical columns.");
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::subselection(
    const Poco::JSON::Object& _col) const {
  const auto data = parse(*JSON::get_object(_col, "operand1_"));

  const auto indices_json = *JSON::get_object(_col, "operand2_");

  const auto type = JSON::get_value<std::string>(indices_json, "type_");

  if (type == FLOAT_COLUMN || type == FLOAT_COLUMN_VIEW) {
    const auto indices = parse(indices_json);

    return containers::ColumnView<Float>::from_numerical_subselection(data,
                                                                      indices);
  }

  const auto indices =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(indices_json);

  return containers::ColumnView<Float>::from_boolean_subselection(data,
                                                                  indices);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::unary_operation(
    const Poco::JSON::Object& _col) const {
  const auto op = JSON::get_value<std::string>(_col, "operator_");

  if (op == "abs") {
    const auto abs = [](const Float val) { return std::abs(val); };
    return un_op(_col, abs);
  }

  if (op == "acos") {
    const auto acos = [](const Float val) { return std::acos(val); };
    return un_op(_col, acos);
  }

  if (op == "as_num") {
    return as_num(_col);
  }

  if (op == "as_ts") {
    return as_ts(_col);
  }

  if (op == "asin") {
    const auto asin = [](const Float val) { return std::asin(val); };
    return un_op(_col, asin);
  }

  if (op == "atan") {
    const auto atan = [](const Float val) { return std::atan(val); };
    return un_op(_col, atan);
  }

  if (op == "boolean_as_num") {
    return boolean_as_num(_col);
  }

  if (op == "cbrt") {
    const auto cbrt = [](const Float val) { return std::cbrt(val); };
    return un_op(_col, cbrt);
  }

  if (op == "ceil") {
    const auto ceil = [](const Float val) { return std::ceil(val); };
    return un_op(_col, ceil);
  }

  if (op == "cos") {
    const auto cos = [](const Float val) { return std::cos(val); };
    return un_op(_col, cos);
  }

  if (op == "day") {
    return un_op(_col, utils::Time::day);
  }

  if (op == "erf") {
    const auto erf = [](const Float val) { return std::erf(val); };
    return un_op(_col, erf);
  }

  if (op == "exp") {
    const auto exp = [](const Float val) { return std::exp(val); };
    return un_op(_col, exp);
  }

  if (op == "floor") {
    const auto floor = [](const Float val) { return std::floor(val); };
    return un_op(_col, floor);
  }

  if (op == "hour") {
    return un_op(_col, utils::Time::hour);
  }

  if (op == "lgamma") {
    const auto lgamma = [](const Float val) { return std::lgamma(val); };
    return un_op(_col, lgamma);
  }

  if (op == "log") {
    const auto log = [](const Float val) { return std::log(val); };
    return un_op(_col, log);
  }

  if (op == "minute") {
    return un_op(_col, utils::Time::minute);
  }

  if (op == "month") {
    return un_op(_col, utils::Time::month);
  }

  if (op == "random") {
    return random(_col);
  }

  if (op == "round") {
    const auto round = [](const Float val) { return std::round(val); };
    return un_op(_col, round);
  }

  if (op == "rowid") {
    return rowid();
  }

  if (op == "second") {
    return un_op(_col, utils::Time::second);
  }

  if (op == "sin") {
    const auto sin = [](const Float val) { return std::sin(val); };
    return un_op(_col, sin);
  }

  if (op == "sqrt") {
    const auto sqrt = [](const Float val) { return std::sqrt(val); };
    return un_op(_col, sqrt);
  }

  if (op == "tan") {
    const auto tan = [](const Float val) { return std::tan(val); };
    return un_op(_col, tan);
  }

  if (op == "tgamma") {
    const auto tgamma = [](const Float val) { return std::tgamma(val); };
    return un_op(_col, tgamma);
  }

  if (op == "value") {
    return parse(*JSON::get_object(_col, "operand1_"));
  }

  if (op == "weekday") {
    return un_op(_col, utils::Time::weekday);
  }

  if (op == "year") {
    return un_op(_col, utils::Time::year);
  }

  if (op == "yearday") {
    return un_op(_col, utils::Time::yearday);
  }

  throw std::invalid_argument("Operator '" + op +
                              "' not recognized for numerical columns.");

  return un_op(_col, utils::Time::yearday);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::update(
    const Poco::JSON::Object& _col) const {
  const auto operand1 = parse(*JSON::get_object(_col, "operand1_"));

  const auto operand2 = parse(*JSON::get_object(_col, "operand2_"));

  const auto condition =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*JSON::get_object(_col, "condition_"));

  const auto op = [](const Float _val1, const Float _val2, const bool _cond) {
    return _cond ? _val2 : _val1;
  };

  return containers::ColumnView<Float>::from_tern_op(operand1, operand2,
                                                     condition, op);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::with_subroles(
    const Poco::JSON::Object& _col) const {
  const auto col = parse(*JSON::get_object(_col, "operand1_"));

  const auto subroles =
      JSON::array_to_vector<std::string>(JSON::get_array(_col, "subroles_"));

  return col.with_subroles(subroles);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> NumOpParser::with_unit(
    const Poco::JSON::Object& _col) const {
  const auto col = parse(*JSON::get_object(_col, "operand1_"));

  const auto unit = JSON::get_value<std::string>(_col, "unit_");

  return col.with_unit(unit);
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
