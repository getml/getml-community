// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/FloatOpParser.hpp"

#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"

namespace engine {
namespace handlers {

containers::ColumnView<Float> FloatOpParser::arange(
    const FloatArangeOp& _col) const {
  const auto start = _col.get<"start_">();

  const auto stop = _col.get<"stop_">();

  const auto step = _col.get<"step_">();

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
    throw std::runtime_error("arange: step cannot be zero.");
  }

  if ((stop - start) * step < 0.0) {
    throw std::runtime_error(
        "arange: stop - start must have the same sign as step.");
  }

  const auto nrows = std::fmod(stop - start, step) == 0.0
                         ? static_cast<size_t>((stop - start) / step)
                         : static_cast<size_t>((stop - start) / step) + 1;

  return containers::ColumnView<Float>(value_func, nrows);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::as_num(
    const FloatFromStringOp& _cmd) const {
  const auto operand1 =
      StringOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*_cmd.get<"operand1_">());

  const auto to_double = [](const strings::String& _str) {
    const auto [val, success] = io::Parser::to_double(_str.str());
    if (success) {
      return val;
    } else {
      return static_cast<Float>(NAN);
    }
  };

  return containers::ColumnView<Float>::from_un_op(operand1, to_double);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::as_ts(
    const FloatAsTSOp& _cmd) const {
  const auto time_formats = _cmd.get<"time_formats_">();

  const auto operand1 =
      StringOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*_cmd.get<"operand1_">());

  const auto to_time_stamp = [time_formats](const strings::String& _str) {
    auto [val, success] = io::Parser::to_time_stamp(_str.str(), time_formats);

    if (success) {
      return val;
    }

    std::tie(val, success) = io::Parser::to_double(_str.str());

    if (success) {
      return val;
    }

    return static_cast<Float>(NAN);
  };

  return containers::ColumnView<Float>::from_un_op(operand1, to_time_stamp);
}

// ----------------------------------------------------------------------------

void FloatOpParser::check(const containers::Column<Float>& _col,
                          const fct::Ref<const communication::Logger>& _logger,
                          Poco::Net::StreamSocket* _socket) const {
  communication::Warner warner;

  if (_col.size() == 0) {
    warner.send(_socket);
    return;
  }

  const Float length = static_cast<Float>(_col.size());

  const Float num_non_null =
      utils::Aggregations::count(_col.begin(), _col.end());

  const auto share_null = 1.0 - num_non_null / length;

  if (share_null > 0.9) {
    warner.add(std::to_string(share_null * 100.0) +
               "% of all entries of column '" + _col.name() +
               "' are NULL values.");
  }

  for (const auto& warning : warner.warnings()) {
    _logger->log("WARNING: " + warning);
  }

  warner.send(_socket);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::binary_operation(
    const FloatBinaryOp& _cmd) const {
  const auto handle =
      [this](const auto& _literal,
             const FloatBinaryOp& _cmd) -> containers::ColumnView<Float> {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, fct::Literal<"divides">>()) {
      return bin_op(_cmd, std::divides<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"fmod">>()) {
      const auto fmod = [](const Float val1, const Float val2) {
        return std::fmod(val1, val2);
      };
      return bin_op(_cmd, fmod);
    }

    if constexpr (std::is_same<Type, fct::Literal<"minus">>()) {
      return bin_op(_cmd, std::minus<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"multiplies">>()) {
      return bin_op(_cmd, std::multiplies<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"plus">>()) {
      return bin_op(_cmd, std::plus<Float>());
    }

    if constexpr (std::is_same<Type, fct::Literal<"pow">>()) {
      const auto pow = [](const Float val1, const Float val2) {
        return std::pow(val1, val2);
      };
      return bin_op(_cmd, pow);
    }
  };

  return fct::visit(handle, _cmd.get<"operator_">(), _cmd);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::boolean_as_num(
    const FloatFromBooleanOp& _cmd) const {
  const auto operand1 =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*_cmd.get<"operand1_">());

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

containers::ColumnView<Float> FloatOpParser::get_column(
    const FloatColumnOp& _cmd) const {
  const auto name = _cmd.get<"name_">();

  const auto df_name = _cmd.get<"df_name_">();

  const auto it = data_frames_->find(df_name);

  if (it == data_frames_->end()) {
    throw std::runtime_error("Column '" + name + "' is from DataFrame '" +
                             df_name + "', but no such DataFrame exists.");
  }

  const auto role = it->second.role(name);

  if (role != containers::DataFrame::ROLE_NUMERICAL &&
      role != containers::DataFrame::ROLE_TARGET &&
      role != containers::DataFrame::ROLE_UNUSED_FLOAT &&
      role != containers::DataFrame::ROLE_TIME_STAMP) {
    throw std::runtime_error(
        "Column '" + name + "' from DataFrame '" + df_name +
        "' is expected to be a FloatColumn, but it appears to be a "
        "StringColumn. You have most likely changed the type when "
        "assigning a new role.");
  }

  const auto float_col = it->second.float_column(name, role);

  return containers::ColumnView<Float>::from_column(float_col);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::parse(
    const commands::FloatColumnOrFloatColumnView& _cmd) const {
  const auto handle =
      [this](const auto& _cmd) -> containers::ColumnView<Float> {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, FloatArangeOp>()) {
      return arange(_cmd);
    }

    if constexpr (std::is_same<Type, FloatAsTSOp>()) {
      return as_ts(_cmd);
    }

    if constexpr (std::is_same<Type, FloatBinaryOp>()) {
      return binary_operation(_cmd);
    }

    if constexpr (std::is_same<Type, FloatFromBooleanOp>()) {
      return boolean_as_num(_cmd);
    }

    if constexpr (std::is_same<Type, FloatColumnOp>()) {
      return get_column(_cmd);
    }

    if constexpr (std::is_same<Type, FloatConstOp>()) {
      const auto val = fct::get<"value_">(_cmd);
      return containers::ColumnView<Float>::from_value(val);
    }

    if constexpr (std::is_same<Type, FloatFromStringOp>()) {
      return as_num(_cmd);
    }

    if constexpr (std::is_same<Type, FloatRandomOp>()) {
      return random(_cmd);
    }

    if constexpr (std::is_same<Type, FloatUnaryOp>()) {
      return unary_operation(_cmd);
    }

    if constexpr (std::is_same<Type, FloatUpdateOp>()) {
      return update(_cmd);
    }

    if constexpr (std::is_same<Type, FloatWithSubrolesOp>()) {
      return with_subroles(_cmd);
    }

    if constexpr (std::is_same<Type, FloatWithUnitOp>()) {
      return with_unit(_cmd);
    }

    if constexpr (std::is_same<Type, FloatSubselectionOp>()) {
      return subselection(_cmd);
    }
  };

  return std::visit(handle, _cmd.val_);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::subselection(
    const FloatSubselectionOp& _cmd) const {
  const auto handle =
      [this, &_cmd](const auto& _operand2) -> containers::ColumnView<Float> {
    using Type = std::decay_t<decltype(_operand2)>;

    const auto data = parse(*_cmd.get<"operand1_">());

    if constexpr (std::is_same<
                      Type,
                      fct::Ref<commands::FloatColumnOrFloatColumnView>>()) {
      const auto indices = parse(*_operand2);
      return containers::ColumnView<Float>::from_numerical_subselection(
          data, indices);
    } else {
      const auto indices =
          BoolOpParser(categories_, join_keys_encoding_, data_frames_)
              .parse(*_operand2);
      return containers::ColumnView<Float>::from_boolean_subselection(data,
                                                                      indices);
    }
  };

  return std::visit(handle, _cmd.get<"operand2_">());
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::unary_operation(
    const FloatUnaryOp& _cmd) const {
  const auto handle =
      [this](const auto& _literal,
             const FloatUnaryOp& _cmd) -> containers::ColumnView<Float> {
    using Type = std::decay_t<decltype(_literal)>;

    if constexpr (std::is_same<Type, fct::Literal<"abs">>()) {
      const auto abs = [](const Float val) { return std::abs(val); };
      return un_op(_cmd, abs);
    }

    if constexpr (std::is_same<Type, fct::Literal<"acos">>()) {
      const auto acos = [](const Float val) { return std::acos(val); };
      return un_op(_cmd, acos);
    }

    if constexpr (std::is_same<Type, fct::Literal<"asin">>()) {
      const auto asin = [](const Float val) { return std::asin(val); };
      return un_op(_cmd, asin);
    }

    if constexpr (std::is_same<Type, fct::Literal<"atan">>()) {
      const auto atan = [](const Float val) { return std::atan(val); };
      return un_op(_cmd, atan);
    }

    if constexpr (std::is_same<Type, fct::Literal<"cbrt">>()) {
      const auto cbrt = [](const Float val) { return std::cbrt(val); };
      return un_op(_cmd, cbrt);
    }

    if constexpr (std::is_same<Type, fct::Literal<"ceil">>()) {
      const auto ceil = [](const Float val) { return std::ceil(val); };
      return un_op(_cmd, ceil);
    }

    if constexpr (std::is_same<Type, fct::Literal<"cos">>()) {
      const auto cos = [](const Float val) { return std::cos(val); };
      return un_op(_cmd, cos);
    }

    if constexpr (std::is_same<Type, fct::Literal<"day">>()) {
      return un_op(_cmd, utils::Time::day);
    }

    if constexpr (std::is_same<Type, fct::Literal<"erf">>()) {
      const auto erf = [](const Float val) { return std::erf(val); };
      return un_op(_cmd, erf);
    }

    if constexpr (std::is_same<Type, fct::Literal<"exp">>()) {
      const auto exp = [](const Float val) { return std::exp(val); };
      return un_op(_cmd, exp);
    }

    if constexpr (std::is_same<Type, fct::Literal<"floor">>()) {
      const auto floor = [](const Float val) { return std::floor(val); };
      return un_op(_cmd, floor);
    }

    if constexpr (std::is_same<Type, fct::Literal<"hour">>()) {
      return un_op(_cmd, utils::Time::hour);
    }

    if constexpr (std::is_same<Type, fct::Literal<"lgamma">>()) {
      const auto lgamma = [](const Float val) { return std::lgamma(val); };
      return un_op(_cmd, lgamma);
    }

    if constexpr (std::is_same<Type, fct::Literal<"log">>()) {
      const auto log = [](const Float val) { return std::log(val); };
      return un_op(_cmd, log);
    }

    if constexpr (std::is_same<Type, fct::Literal<"minute">>()) {
      return un_op(_cmd, utils::Time::minute);
    }

    if constexpr (std::is_same<Type, fct::Literal<"month">>()) {
      return un_op(_cmd, utils::Time::month);
    }

    if constexpr (std::is_same<Type, fct::Literal<"round">>()) {
      const auto round = [](const Float val) { return std::round(val); };
      return un_op(_cmd, round);
    }

    if constexpr (std::is_same<Type, fct::Literal<"rowid">>()) {
      return rowid();
    }

    if constexpr (std::is_same<Type, fct::Literal<"second">>()) {
      return un_op(_cmd, utils::Time::second);
    }

    if constexpr (std::is_same<Type, fct::Literal<"sin">>()) {
      const auto sin = [](const Float val) { return std::sin(val); };
      return un_op(_cmd, sin);
    }

    if constexpr (std::is_same<Type, fct::Literal<"sqrt">>()) {
      const auto sqrt = [](const Float val) { return std::sqrt(val); };
      return un_op(_cmd, sqrt);
    }

    if constexpr (std::is_same<Type, fct::Literal<"tan">>()) {
      const auto tan = [](const Float val) { return std::tan(val); };
      return un_op(_cmd, tan);
    }

    if constexpr (std::is_same<Type, fct::Literal<"tgamma">>()) {
      const auto tgamma = [](const Float val) { return std::tgamma(val); };
      return un_op(_cmd, tgamma);
    }

    if constexpr (std::is_same<Type, fct::Literal<"sqrt">>()) {
      return parse(*_cmd.get<"operand1_">());
    }

    if constexpr (std::is_same<Type, fct::Literal<"weekday">>()) {
      return un_op(_cmd, utils::Time::weekday);
    }

    if constexpr (std::is_same<Type, fct::Literal<"year">>()) {
      return un_op(_cmd, utils::Time::year);
    }

    if constexpr (std::is_same<Type, fct::Literal<"yearday">>()) {
      return un_op(_cmd, utils::Time::yearday);
    }
  };

  return fct::visit(handle, _cmd.get<"operator_">(), _cmd);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::update(
    const FloatUpdateOp& _cmd) const {
  const auto operand1 = parse(*_cmd.get<"operand1_">());

  const auto operand2 = parse(*_cmd.get<"operand2_">());

  const auto condition =
      BoolOpParser(categories_, join_keys_encoding_, data_frames_)
          .parse(*_cmd.get<"condition_">());

  const auto op = [](const auto& _val1, const auto& _val2,
                     const bool _cond) -> Float {
    return _cond ? _val2 : _val1;
  };

  return containers::ColumnView<Float>::from_tern_op(operand1, operand2,
                                                     condition, op);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::with_subroles(
    const FloatWithSubrolesOp& _cmd) const {
  const auto col = parse(*_cmd.get<"operand1_">());
  const auto subroles = _cmd.get<"subroles_">();
  return col.with_subroles(subroles);
}

// ----------------------------------------------------------------------------

containers::ColumnView<Float> FloatOpParser::with_unit(
    const FloatWithUnitOp& _cmd) const {
  const auto col = parse(*_cmd.get<"operand1_">());
  const auto unit = _cmd.get<"unit_">();
  return col.with_unit(unit);
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
