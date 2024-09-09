// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "transpilation/SQLGenerator.hpp"

#include <ranges>
#include <sstream>

#include "debug/throw_unless.hpp"
#include "helpers/Macros.hpp"
#include "helpers/StringReplacer.hpp"
#include "helpers/StringSplitter.hpp"

// ----------------------------------------------------------------------------

namespace transpilation {

std::string SQLGenerator::get_table_name(const std::string& _raw_name) {
  const auto has_delimiter =
      (_raw_name.find(helpers::Macros::delimiter()) != std::string::npos);

  auto name = has_delimiter ? helpers::StringSplitter::split(
                                  _raw_name, helpers::Macros::delimiter())
                                  .at(0)
                            : _raw_name;

  name = helpers::StringReplacer::replace_all(
      name, helpers::Macros::population(), "");

  name = helpers::StringReplacer::replace_all(
      name, helpers::Macros::peripheral(), "");

  const auto pos = name.find(helpers::Macros::staging_table_num());

  if (pos == std::string::npos) {
    return name;
  }

  return name.substr(0, pos);
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::handle_many_to_one_joins(
    const std::string& _table_name, const std::string& _t1_or_t2,
    const SQLDialectGenerator* _sql_dialect_generator) {
  if (_table_name.find(helpers::Macros::delimiter()) == std::string::npos) {
    return "";
  }

  const auto quote1 = _sql_dialect_generator->quotechar1();

  const auto quote2 = _sql_dialect_generator->quotechar2();

  const auto joins =
      helpers::StringSplitter::split(_table_name, helpers::Macros::delimiter());

  std::stringstream sql;

  for (size_t i = 1; i < joins.size(); ++i) {
    const auto [name, alias, join_key, other_join_key, time_stamp,
                other_time_stamp, upper_time_stamp, joined_to_name,
                joined_to_alias, _] =
        helpers::Macros::parse_table_name(joins.at(i));

    sql << "LEFT JOIN " << _sql_dialect_generator->schema() << quote1 << name
        << quote2 << " " << alias << std::endl;

    sql << handle_multiple_join_keys(join_key, other_join_key, joined_to_alias,
                                     alias, FOR_STAGING,
                                     _sql_dialect_generator);

    if (time_stamp != "" && other_time_stamp != "") {
      sql << "AND "
          << _sql_dialect_generator->make_time_stamps(
                 time_stamp, other_time_stamp, upper_time_stamp,
                 joined_to_alias, alias, _t1_or_t2);
    }
  }

  return helpers::StringReplacer::replace_all(
      sql.str(), helpers::Macros::t1_or_t2(), _t1_or_t2);
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::handle_multiple_join_keys(
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name, const std::string& _output_alias,
    const std::string& _input_alias, const bool _for_staging,
    const SQLDialectGenerator* _sql_dialect_generator) {
  const auto sep = helpers::Macros::multiple_join_key_sep();

  auto join_keys1 = helpers::StringSplitter::split(_output_join_keys_name, sep);

  auto join_keys2 = helpers::StringSplitter::split(_input_join_keys_name, sep);

  throw_unless(
      join_keys1.size() == join_keys2.size(),
      "Error while handling multiple join keys: Number of join keys does "
      "not match: " +
          std::to_string(join_keys1.size()) + " vs. " +
          std::to_string(join_keys2.size()));

  if (join_keys1.size() > 1) {
    join_keys1.front() = helpers::StringReplacer::replace_all(
        join_keys1.front(), helpers::Macros::multiple_join_key_begin(), "");
    join_keys1.back() = helpers::StringReplacer::replace_all(
        join_keys1.back(), helpers::Macros::multiple_join_key_end(), "");
    join_keys2.front() = helpers::StringReplacer::replace_all(
        join_keys2.front(), helpers::Macros::multiple_join_key_begin(), "");
    join_keys2.back() = helpers::StringReplacer::replace_all(
        join_keys2.back(), helpers::Macros::multiple_join_key_end(), "");
  }

  std::stringstream sql;

  sql << "ON ";

  for (size_t i = 0; i < join_keys1.size(); ++i) {
    if (_for_staging) {
      sql << _sql_dialect_generator->make_staging_table_column(join_keys1.at(i),
                                                               _output_alias);

      sql << " = ";

      sql << _sql_dialect_generator->make_staging_table_column(join_keys2.at(i),
                                                               _input_alias);

      sql << std::endl;
    } else {
      const auto quote1 = _sql_dialect_generator->quotechar1();

      const auto quote2 = _sql_dialect_generator->quotechar2();

      sql << _output_alias << "." << quote1
          << _sql_dialect_generator->make_staging_table_colname(
                 join_keys1.at(i))
          << quote2;

      sql << " = ";

      sql << _input_alias << "." << quote1
          << _sql_dialect_generator->make_staging_table_colname(
                 join_keys2.at(i))
          << quote2;

      sql << std::endl;
    }

    if (i != join_keys1.size() - 1) {
      sql << "AND ";
    }
  }

  return sql.str();
}

// ----------------------------------------------------------------------------

bool SQLGenerator::include_column(const std::string& _name) {
  if (_name == helpers::Macros::no_join_key()) {
    return false;
  }

  if (_name == helpers::Macros::self_join_key()) {
    return false;
  }

  if (_name.find(helpers::Macros::multiple_join_key_begin()) !=
      std::string::npos) {
    return false;
  }

  if (_name.find("__mapping_") != std::string::npos) {
    return false;
  }

  return true;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_postprocessing(
    const std::vector<std::string>& _sql) {
  std::string sql;

  for (const auto& feature : _sql) {
    const auto pos = feature.find("\";\n");

    throw_unless(pos != std::string::npos,
                 "Could not find end of DROP TABLE IF EXISTS statement.");

    sql += feature.substr(0, pos) + "\";\n";
  }

  return sql;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_staging_table_name(const std::string& _name) {
  const auto pos = _name.find(helpers::Macros::staging_table_num());

  if (pos == std::string::npos) {
    return to_upper(get_table_name(_name));
  }

  const auto begin = pos + helpers::Macros::staging_table_num().size();

  const auto end = _name.find_first_not_of("0123456789", begin);

  assert_true(end > begin);

  const auto number = _name.substr(begin, end - begin);

  const auto pos_text_field = _name.find(helpers::Macros::text_field());

  const auto text_field_col =
      (pos_text_field == std::string::npos)
          ? std::string("")
          : "__" + _name.substr(pos_text_field +
                                helpers::Macros::text_field().size());

  return to_upper(get_table_name(_name)) + "__STAGING_TABLE_" + number +
         to_upper(text_field_col);
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_subfeature_identifier(
    const std::string& _feature_prefix, const size_t _peripheral_used) {
  return _feature_prefix + std::to_string(_peripheral_used + 1);
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_time_stamp_diff(const Float _diff,
                                               const bool _is_rowid) {
  const auto diffstr = [_diff]() {
    constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
    constexpr Float seconds_per_hour = 60.0 * 60.0;
    constexpr Float seconds_per_minute = 60.0;

    const auto abs_diff = std::abs(_diff);

    if (abs_diff >= seconds_per_day) {
      return std::to_string(_diff / seconds_per_day) + " days";
    }

    if (abs_diff >= seconds_per_hour) {
      return std::to_string(_diff / seconds_per_hour) + " hours";
    }

    if (abs_diff >= seconds_per_minute) {
      return std::to_string(_diff / seconds_per_minute) + " minutes";
    }

    return std::to_string(_diff) + " seconds";
  };

  if (_is_rowid) {
    return helpers::Macros::diffstr() + " + " + std::to_string(_diff);
  }

  const std::string sign = _diff >= 0.0 ? "+" : "";

  return helpers::Macros::diffstr() + ", '" + sign + diffstr() + "'";
}

// ----------------------------------------------------------------------------

Float SQLGenerator::parse_time_stamp_diff(const std::string& _diff) {
  constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;

  constexpr Float seconds_per_hour = 60.0 * 60.0;

  constexpr Float seconds_per_minute = 60.0;

  const auto trimmed = [&_diff]() -> std::string {
    const auto pos = _diff.find_first_of("0123456789-");
    return _diff.substr(pos);
  };

  const auto val = std::stod(trimmed());

  if (_diff.find("days'") != std::string::npos) {
    return val * seconds_per_day;
  }

  if (_diff.find("hours'") != std::string::npos) {
    return val * seconds_per_hour;
  }

  if (_diff.find("minutes'") != std::string::npos) {
    return val * seconds_per_minute;
  }

  return val;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::replace_non_alphanumeric(const std::string _old) {
  const auto replace = [](const char c) -> char {
    if (c >= 'a' && c <= 'z') {
      return c;
    }

    if (c >= 'A' && c <= 'Z') {
      return c;
    }

    if (c >= '0' && c <= '9') {
      return c;
    }

    return '_';
  };

  const auto trim = [](const std::string& _str) -> std::string {
    const auto pos1 = _str.find_first_not_of("_");

    const auto pos2 = _str.find_last_not_of("_");

    if (pos2 <= pos1 || pos2 == std::string::npos) {
      return _str;
    }

    const auto length = pos2 - pos1 + 1;

    return _str.substr(pos1, length);
  };

  const auto shorten = [](const std::string& _str) -> std::string {
    auto str = _str;

    while (str.find("___") != std::string::npos) {
      str = helpers::StringReplacer::replace_all(str, "___", "__");
    }

    return str;
  };

  const auto replaced =
      _old | std::views::transform(replace) | std::ranges::to<std::string>();

  return shorten(trim(replaced));
};

// ------------------------------------------------------------------------

std::string SQLGenerator::to_lower(const std::string& _str) {
  auto lower = _str;

  bool skip = false;

  for (auto& c : lower) {
    if (skip) {
      skip = false;
      continue;
    }

    if (c == '%') {
      skip = true;
    }

    c = std::tolower(c);
  }

  return lower;
}

// ------------------------------------------------------------------------

std::string SQLGenerator::to_upper(const std::string& _str) {
  auto upper = _str;

  bool skip = false;

  for (auto& c : upper) {
    if (skip) {
      skip = false;
      continue;
    }

    if (c == '%') {
      skip = true;
    }

    c = std::toupper(c);
  }

  return upper;
}

// ----------------------------------------------------------------------------
}  // namespace transpilation
