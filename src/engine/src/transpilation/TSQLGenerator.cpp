#include "transpilation/TSQLGenerator.hpp"

// ----------------------------------------------------------------------------

#include "helpers/ColumnDescription.hpp"
#include "helpers/Macros.hpp"
#include "helpers/Schema.hpp"
#include "helpers/StringReplacer.hpp"
#include "helpers/enums/Aggregation.hpp"
#include "helpers/enums/enums.hpp"
#include "stl/stl.hpp"

// ----------------------------------------------------------------------------

#include "transpilation/SQLGenerator.hpp"

// ----------------------------------------------------------------------------

namespace transpilation {

std::string TSQLGenerator::aggregation(
    const helpers::enums::Aggregation& _agg, const std::string& _colname1,
    const std::optional<std::string>& _colname2) const {
  if (_agg == helpers::enums::Aggregation::avg_time_between) {
    assert_true(_colname2);

    return "CASE WHEN COUNT( * ) > 1 THEN ( MAX( " + _colname2.value() +
           " ) - MIN ( " + _colname2.value() +
           " ) ) / ( COUNT( * ) - 1 )  ELSE 0 END";
  }

  const auto value = _colname2 ? _colname1 + ", " + *_colname2 : _colname1;

  switch (_agg) {
    case helpers::enums::Aggregation::avg_time_between:
      assert_true(_colname2);
      return "CASE WHEN COUNT( * ) > 1 THEN ( MAX( " + *_colname2 +
             " ) - MIN ( " + *_colname2 +
             " ) ) / ( COUNT( * ) - 1 )  ELSE 0 END";

    case helpers::enums::Aggregation::count_distinct:
      return "COUNT( DISTINCT " + value + " )";

    case helpers::enums::Aggregation::count_distinct_over_count:
      return "CASE WHEN COUNT( " + _colname1 +
             ") = 0 THEN 0.0 ELSE CAST( COUNT( DISTINCT " + _colname1 +
             " ) AS DOUBLE PRECISION ) / CAST( COUNT( " + _colname1 +
             " ) AS DOUBLE PRECISION ) END";

    case helpers::enums::Aggregation::count_minus_count_distinct:
      return "COUNT( " + value + "  ) - COUNT( DISTINCT " + value + " )";

    case helpers::enums::Aggregation::ewma1s:
    case helpers::enums::Aggregation::ewma1m:
    case helpers::enums::Aggregation::ewma1h:
    case helpers::enums::Aggregation::ewma1d:
    case helpers::enums::Aggregation::ewma7d:
    case helpers::enums::Aggregation::ewma30d:
    case helpers::enums::Aggregation::ewma90d:
    case helpers::enums::Aggregation::ewma365d:
      assert_true(_colname2);
      return make_ewma_aggregation(_agg, _colname1, _colname2.value());

    case helpers::enums::Aggregation::first:
      assert_true(_colname2);
      return first_last_aggregation(_colname1, _colname2.value(), true);

    case helpers::enums::Aggregation::kurtosis:
      return make_kurtosis_aggregation(_colname1);

    case helpers::enums::Aggregation::last:
      assert_true(_colname2);
      return first_last_aggregation(_colname1, _colname2.value(), false);

    case helpers::enums::Aggregation::median:
      return make_percentile_aggregation(_colname1, "0.5");

    case helpers::enums::Aggregation::mode:
      return make_mode_aggregation(value);

    case helpers::enums::Aggregation::num_max:
      return num_max_min_aggregation(_colname1, true);

    case helpers::enums::Aggregation::num_min:
      return num_max_min_aggregation(_colname1, false);

    case helpers::enums::Aggregation::q1:
      return make_percentile_aggregation(_colname1, "0.01");

    case helpers::enums::Aggregation::q5:
      return make_percentile_aggregation(_colname1, "0.05");

    case helpers::enums::Aggregation::q10:
      return make_percentile_aggregation(_colname1, "0.1");

    case helpers::enums::Aggregation::q25:
      return make_percentile_aggregation(_colname1, "0.25");

    case helpers::enums::Aggregation::q75:
      return make_percentile_aggregation(_colname1, "0.75");

    case helpers::enums::Aggregation::q90:
      return make_percentile_aggregation(_colname1, "0.9");

    case helpers::enums::Aggregation::q95:
      return make_percentile_aggregation(_colname1, "0.95");

    case helpers::enums::Aggregation::q99:
      return make_percentile_aggregation(_colname1, "0.99");

    case helpers::enums::Aggregation::skew:
      return make_skewness_aggregation(_colname1);

    case helpers::enums::Aggregation::time_since_first_maximum:
      assert_true(_colname2);
      return first_or_last_optimum_aggregation(_colname1, _colname2.value(),
                                               true, false);

    case helpers::enums::Aggregation::time_since_first_minimum:
      assert_true(_colname2);
      return first_or_last_optimum_aggregation(_colname1, _colname2.value(),
                                               true, true);

    case helpers::enums::Aggregation::time_since_last_maximum:
      assert_true(_colname2);
      return first_or_last_optimum_aggregation(_colname1, _colname2.value(),
                                               false, false);

    case helpers::enums::Aggregation::time_since_last_minimum:
      assert_true(_colname2);
      return first_or_last_optimum_aggregation(_colname1, _colname2.value(),
                                               false, true);
    case helpers::enums::Aggregation::trend:
      assert_true(_colname2);
      return make_trend_aggregation(_colname1, _colname2.value());

    case helpers::enums::Aggregation::stddev:
      return "SQRT( VARP( " + _colname1 + " ) )";

    case helpers::enums::Aggregation::var:
      return "VARP( " + _colname1 + " )";

    case helpers::enums::Aggregation::variation_coefficient:
      return "CASE WHEN AVG( " + _colname1 + " ) != 0 THEN VARP( " + _colname1 +
             " ) / AVG( " + _colname1 + " ) ELSE NULL END";

    default:
      const auto agg_type =
          helpers::enums::Parser<helpers::enums::Aggregation>::to_str(_agg);

      return helpers::StringReplacer::replace_all(agg_type, " ", "_") + "( " +
             value + " )";
  }
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::drop_table_if_exists(
    const std::string& _table_name) const {
  std::stringstream sql;
  sql << "DROP TABLE IF EXISTS " << schema() << quotechar1() << _table_name
      << quotechar2() << ";" << std::endl
      << std::endl;
  return sql.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::first_last_aggregation(const std::string& _colname1,
                                                  const std::string& _colname2,
                                                  const bool _first) const {
  const std::string ts_compare = _first ? "ASC" : "DESC";

  std::stringstream stream;

  stream << "FIRST_VALUE( " << _colname1 << " ) OVER ( PARTITION BY t1."
         << rowid() << " ORDER BY " << _colname2 << " " << ts_compare << " )";

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::first_or_last_optimum_aggregation(
    const std::string& _colname1, const std::string& _colname2,
    const bool _is_first, const bool _is_minimum) const {
  const std::string ts_compare = _is_first ? "DESC" : "ASC";

  const std::string value_compare = _is_minimum ? "ASC" : "DESC";

  std::stringstream stream;

  stream << "FIRST_VALUE( " << _colname2 << " ) OVER ( PARTITION BY t1."
         << rowid() << " ORDER BY " << _colname1 << " " << value_compare << ", "
         << _colname2 << " " << ts_compare << " )";

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::handle_escape_char(const char c) const {
  switch (c) {
    case '\t':
      return "\\t";

    case '\"':
      return "\\\"";

    case '\v':
      return "\\v";

    case '\r':
      return "\\r";

    case '\n':
      return "\\n";

    case '\f':
      return "\\f";

    case '\'':
      return "''";

    case ';':
      return "";

    default:
      return std::string(1, c);
  }
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::create_indices(
    const std::string& _table_name, const helpers::Schema& _schema) const {
  const auto create_index =
      [this, &_table_name](const std::string& _colname) -> std::string {
    const auto colname = make_staging_table_colname(_colname);
    const auto index_name = _table_name + "__" + colname;
    std::stringstream stream;
    stream << "DROP INDEX IF EXISTS " << quotechar1() << index_name
           << quotechar2() << " ON " << schema() << quotechar1() << _table_name
           << quotechar2() << ";" << std::endl
           << std::endl
           << "CREATE INDEX " << quotechar1() << index_name << quotechar2()
           << " ON " << schema() << quotechar1() << _table_name << quotechar2()
           << " (" << quotechar1() << colname << quotechar2() << ");"
           << std::endl
           << std::endl;
    return stream.str();
  };

  return stl::collect::string(_schema.join_keys_ |
                              VIEWS::filter(SQLGenerator::include_column) |
                              VIEWS::transform(create_index)) +
         stl::collect::string(_schema.time_stamps_ |
                              VIEWS::transform(create_index));
}

// ----------------------------------------------------------------------------

std::optional<std::string> TSQLGenerator::make_outer_aggregation(
    const helpers::enums::Aggregation& _agg) const {
  switch (_agg) {
    case helpers::enums::Aggregation::first:
    case helpers::enums::Aggregation::last:
    case helpers::enums::Aggregation::median:
    case helpers::enums::Aggregation::q1:
    case helpers::enums::Aggregation::q5:
    case helpers::enums::Aggregation::q10:
    case helpers::enums::Aggregation::q25:
    case helpers::enums::Aggregation::q75:
    case helpers::enums::Aggregation::q90:
    case helpers::enums::Aggregation::q95:
    case helpers::enums::Aggregation::q99:
    case helpers::enums::Aggregation::time_since_first_maximum:
    case helpers::enums::Aggregation::time_since_first_minimum:
    case helpers::enums::Aggregation::time_since_last_maximum:
    case helpers::enums::Aggregation::time_since_last_minimum:
      return "MIN";

    case helpers::enums::Aggregation::num_max:
    case helpers::enums::Aggregation::num_min:
      return "SUM";

    default:
      return std::nullopt;
  }
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::create_table(const helpers::enums::Aggregation& _agg,
                                        const std::string& _feature_prefix,
                                        const std::string& _feature_num) const {
  const auto col_name = "feature_" + _feature_prefix + _feature_num;
  const auto table_name = "FEATURE_" + _feature_prefix + _feature_num;
  const auto outer_aggregation = make_outer_aggregation(_agg);
  const auto aggregated = outer_aggregation
                              ? *outer_aggregation + "( t0." + quotechar1() +
                                    col_name + quotechar2() + " )"
                              : "t0." + quotechar1() + col_name + quotechar2();
  std::stringstream sql;
  sql << "SELECT " << aggregated << " AS " << quotechar1() << col_name
      << quotechar2() << ", " << std::endl
      << "       t0." << rowid() << " AS " << rowid() << std::endl
      << "INTO " << schema() << quotechar1() << table_name << quotechar2()
      << " FROM (" << std::endl;
  return sql.str();
}

// ----------------------------------------------------------------------------

std::tuple<std::string, std::string, std::string>
TSQLGenerator::demangle_colname(const std::string& _raw_name) const {
  const auto m_pos = _raw_name.find("__mapping_");

  auto new_name = (m_pos != std::string::npos)
                      ? make_staging_table_colname(_raw_name.substr(0, m_pos)) +
                            _raw_name.substr(m_pos)
                      : _raw_name;

  new_name = helpers::Macros::prefix() + new_name + helpers::Macros::postfix();

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::generated_ts(), "");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::rowid(), "rowid");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::open_bracket(),
      "( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::close_bracket(),
      helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::email_domain_begin(),
      "email_domain( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::email_domain_end(),
      helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::imputation_begin(),
      "COALESCE( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::imputation_replacement(),
      helpers::Macros::postfix() + ", ");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::imputation_end(),
      helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::dummy_begin(),
      "( CASE WHEN " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::dummy_end(),
      helpers::Macros::postfix() + " IS NULL THEN 1 ELSE 0 END )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::diffstr(), helpers::Macros::postfix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::substring(),
      "substring( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::begin(), helpers::Macros::postfix() + ", ");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::length(), helpers::Macros::postfix() + ", ");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::hour_begin(),
      "right( '0' + CAST( datepart( hour, " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::hour_end(),
      helpers::Macros::postfix() + " ) AS VARCHAR(2) ), 2 )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::minute_begin(),
      "right( '0' + CAST( datepart( minute, " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::minute_end(),
      helpers::Macros::postfix() + " ) AS VARCHAR(2) ), 2 )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::month_begin(),
      "right( '0' + CAST( datepart( month, " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::month_end(),
      helpers::Macros::postfix() + " ) AS VARCHAR(2) ), 2 )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::weekday_begin(),
      "( datepart( weekday, " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::weekday_end(),
      helpers::Macros::postfix() +
          " ) - datepart( weekday, '2007-04-22' ) + 7 ) % 7");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::year_begin(),
      "datepart( year, " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::year_end(), helpers::Macros::postfix() + " )");

  const auto pos1 = new_name.rfind(helpers::Macros::prefix()) +
                    helpers::Macros::prefix().size();

  const auto pos2 = new_name.find(helpers::Macros::postfix());

  throw_unless(pos2 >= pos1, "Error: Macros in colname do not make sense!");

  const auto length = pos2 - pos1;

  const auto prefix = helpers::StringReplacer::replace_all(
      new_name.substr(0, pos1), helpers::Macros::prefix(), "");

  const auto postfix = helpers::StringReplacer::replace_all(
      new_name.substr(pos2), helpers::Macros::postfix(), "");

  new_name = new_name.substr(pos1, length);

  const auto has_col_param =
      (new_name.find(helpers::Macros::column()) != std::string::npos);

  new_name = has_col_param ? helpers::Macros::get_param(
                                 new_name, helpers::Macros::column())
                           : new_name;

  const auto [edited_prefix, edited_postfix] =
      edit_prefix_postfix(_raw_name, prefix, postfix);

  return std::make_tuple(edited_prefix, new_name, edited_postfix);
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_staging_table_column(
    const std::string& _raw_name, const std::string& _alias) const {
  if (_raw_name.find(helpers::Macros::no_join_key()) != std::string::npos) {
    return "1";
  }

  if (_raw_name.find(helpers::Macros::self_join_key()) != std::string::npos) {
    return "1";
  }

  const auto [prefix, new_name, postfix] = demangle_colname(_raw_name);

  const bool need_alias = (_alias != "");

  const bool has_alias =
      (_raw_name.find(helpers::Macros::alias()) != std::string::npos);

  const bool not_t1_or_t2 =
      has_alias &&
      (helpers::Macros::get_param(_raw_name, helpers::Macros::alias()) !=
       helpers::Macros::t1_or_t2());

  const bool extract_alias = need_alias && has_alias && not_t1_or_t2;

  const auto alias = extract_alias ? helpers::Macros::get_param(
                                         _raw_name, helpers::Macros::alias())
                                   : _alias;

  const auto dot = (alias == "") ? "" : ".";

  const auto quotation1 = alias == "" ? "" : quotechar1();

  const auto quotation2 = alias == "" ? "" : quotechar2();

  return prefix + alias + dot + quotation1 + new_name + quotation2 + postfix;
}

// ----------------------------------------------------------------------------

std::pair<std::string, std::string> TSQLGenerator::edit_prefix_postfix(
    const std::string& _raw_name, const std::string& _prefix,
    const std::string& _postfix) const {
  const bool editing_required =
      _raw_name.find(helpers::Macros::diffstr()) != std::string::npos &&
      _raw_name.find(helpers::Macros::rowid()) == std::string::npos;

  if (!editing_required) {
    return std::make_pair(_prefix, _postfix);
  }

  const auto make_diff = [](const std::string& _postfix) {
    const auto diff_float = SQLGenerator::parse_time_stamp_diff(_postfix);
    const auto days = static_cast<std::int32_t>(diff_float) / 86400;

    const auto seconds_float = SQLGenerator::parse_time_stamp_diff(_postfix);
    const auto seconds = static_cast<std::int32_t>(seconds_float) % 86400;

    const auto fractional_float =
        (diff_float - static_cast<Float>(86400 * days + seconds)) * 1e9;
    const auto fractional = static_cast<std::int32_t>(fractional_float);

    return std::make_tuple(days, seconds, fractional);
  };

  const auto edited_prefix = [make_diff, &_prefix, _postfix]() -> std::string {
    const auto [days, seconds, fractional] = make_diff(_postfix);

    const auto days_str = days == 0
                              ? std::string()
                              : "DATEADD( day, " + std::to_string(days) + ", ";

    const auto seconds_str =
        seconds == 0 ? std::string()
                     : "DATEADD( second, " + std::to_string(seconds) + ", ";

    const auto fractional_str =
        fractional == 0
            ? std::string()
            : "DATEADD( nanosecond, " + std::to_string(fractional) + ", ";

    return _prefix + days_str + seconds_str + fractional_str;
  };

  const auto edited_postfix = [make_diff, &_postfix]() -> std::string {
    const auto [days, seconds, fractional] = make_diff(_postfix);

    const auto days_str = days == 0 ? std::string() : " )";

    const auto seconds_str = seconds == 0 ? std::string() : " )";

    const auto fractional_str = fractional == 0 ? std::string() : " )";

    const auto closing_brackets = days_str + seconds_str + fractional_str;

    const auto pos = _postfix.find(" )");

    if (pos == std::string::npos) {
      return closing_brackets;
    }

    return closing_brackets + _postfix.substr(pos);
  };

  return std::make_pair(edited_prefix(), edited_postfix());
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::group_by(
    const helpers::enums::Aggregation _agg,
    const std::string& _value_to_be_aggregated) const {
  if (_agg == helpers::enums::Aggregation::mode) {
    std::stringstream sql;
    sql << "GROUP BY t1." << rowid() << ", " << _value_to_be_aggregated
        << std::endl
        << ") t0" << std::endl
        << "WHERE " << quotechar1() << "sequence" << quotechar2() << " = 1";
    return sql.str();
  }

  if (make_outer_aggregation(_agg)) {
    std::stringstream sql;
    sql << ") t0" << std::endl << "GROUP BY t0." << rowid();
    return sql.str();
  }

  std::stringstream sql;
  sql << "GROUP BY t1." << rowid() << std::endl << ") t0";
  return sql.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_ewma_aggregation(
    const helpers::enums::Aggregation& _agg, const std::string& _value,
    const std::string& _timestamp) const {
  constexpr Float t1s = 1.0;
  constexpr Float t1m = t1s * 60.0;
  constexpr Float t1h = t1m * 60.0;
  constexpr Float t1d = t1h * 24.0;
  constexpr Float t7d = t1d * 7.0;
  constexpr Float t30d = t1d * 30.0;
  constexpr Float t90d = t1d * 90.0;
  constexpr Float t365d = t1d * 365.0;

  const auto make_ewma = [](const std::string& _value,
                            const std::string& _timestamp,
                            const Float _half_life) -> std::string {
    const auto exp = "EXP( ( " + _timestamp + " ) * LOG( 0.5 ) / " +
                     std::to_string(_half_life) + " )";

    return "CASE WHEN COUNT( " + _value + " ) > 0 THEN SUM( ( " + _value +
           " ) * " + exp + " ) / SUM( " + exp + " ) ELSE NULL END";
  };

  switch (_agg) {
    case helpers::enums::Aggregation::ewma1s:
      return make_ewma(_value, _timestamp, t1s);

    case helpers::enums::Aggregation::ewma1m:
      return make_ewma(_value, _timestamp, t1m);

    case helpers::enums::Aggregation::ewma1h:
      return make_ewma(_value, _timestamp, t1h);

    case helpers::enums::Aggregation::ewma1d:
      return make_ewma(_value, _timestamp, t1d);

    case helpers::enums::Aggregation::ewma7d:
      return make_ewma(_value, _timestamp, t7d);

    case helpers::enums::Aggregation::ewma30d:
      return make_ewma(_value, _timestamp, t30d);

    case helpers::enums::Aggregation::ewma90d:
      return make_ewma(_value, _timestamp, t90d);

    case helpers::enums::Aggregation::ewma365d:
      return make_ewma(_value, _timestamp, t365d);

    default:
      assert_true(false);
      return "";
  }
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_mode_aggregation(
    const std::string& _colname) const {
  std::stringstream stream;

  stream << "ROW_NUMBER() OVER ( PARTITION BY t1." << rowid()
         << " ORDER BY COUNT(*) DESC, " << _colname << " ASC ) AS "
         << quotechar1() << "sequence" << quotechar2() << "," << std::endl
         << "       " << _colname;

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_staging_table_colname(
    const std::string& _raw_name) const {
  const auto [prefix, new_name, postfix] = demangle_colname(_raw_name);

  const bool has_alias =
      (_raw_name.find(helpers::Macros::alias()) != std::string::npos);

  const bool not_t1_or_t2 =
      has_alias &&
      (helpers::Macros::get_param(_raw_name, helpers::Macros::alias()) !=
       helpers::Macros::t1_or_t2());

  const bool is_not_mapping =
      (_raw_name.find("__mapping_") == std::string::npos);

  const bool extract_alias = has_alias && not_t1_or_t2 && is_not_mapping;

  const auto alias = extract_alias ? helpers::Macros::get_param(
                                         _raw_name, helpers::Macros::alias())
                                   : "";

  const auto underscore = (alias == "") ? "" : "__";

  const auto almost_final_name =
      alias + underscore + prefix + new_name + postfix;

  return SQLGenerator::to_lower(
      SQLGenerator::replace_non_alphanumeric(almost_final_name));
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::num_max_min_aggregation(const std::string& _colname,
                                                   const bool _max) const {
  const auto compare = _max ? "DESC" : "ASC";

  std::stringstream stream;

  stream << "CASE WHEN " << _colname << " = FIRST_VALUE( " << _colname
         << " ) OVER ( PARTITION BY t1." << rowid() << " ORDER BY " << _colname
         << " " << compare << " ) THEN 1 ELSE 0 END";

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::join_mapping(const std::string& _name,
                                        const std::string& _colname,
                                        const bool _is_text) const {
  const bool is_text_field =
      (_name.find(helpers::Macros::text_field()) != std::string::npos);

  const auto table_name =
      SQLGenerator::to_upper(SQLGenerator::make_staging_table_name(_name));

  const auto mapping_col = SQLGenerator::to_lower(_colname);

  const auto pos = mapping_col.find("__mapping_");

  assert_true(pos != std::string::npos);

  const auto orig_col = mapping_col.substr(0, pos);

  const auto join_text = [this, &table_name, &mapping_col,
                          &orig_col]() -> std::string {
    const auto splitted_table =
        table_name + "__" + SQLGenerator::to_upper(orig_col);

    const auto grouped_table =
        SQLGenerator::to_upper(mapping_col + "__GROUPED");

    const auto desc =
        std::make_shared<helpers::ColumnDescription>("", table_name, orig_col);

    const auto colname =
        SQLGenerator::to_lower(make_staging_table_colname(desc->name_));

    std::stringstream stream;

    stream << split_text_fields(desc, true);

    stream << "SELECT t1." << quotechar1() << "rownum" << quotechar2() << ","
           << std::endl
           << "       AVG( t2.value ) AS value" << std::endl
           << "INTO " << quotechar1() << grouped_table << quotechar2()
           << std::endl
           << "FROM " << schema() << quotechar1() << splitted_table
           << quotechar2() << " t1" << std::endl
           << "LEFT JOIN " << schema() << quotechar1()
           << SQLGenerator::to_upper(mapping_col) << quotechar2() << " t2"
           << std::endl
           << "ON t1." << quotechar1() << orig_col << quotechar2() << " = t2."
           << quotechar1() << "key" << quotechar2() << std::endl
           << "GROUP BY t1." << quotechar1() << "rownum" << quotechar2() << ";"
           << std::endl
           << std::endl;

    stream << "UPDATE " << schema() << quotechar1() << table_name
           << quotechar2() << std::endl
           << "SET " << quotechar1() << mapping_col << quotechar2() << " = t2."
           << quotechar1() << "value" << quotechar2() << std::endl
           << "FROM " << schema() << quotechar1() << grouped_table
           << quotechar2() << " AS t2" << std::endl
           << "WHERE " << quotechar1() << table_name << quotechar2() << "."
           << rowid() << " = t2." << quotechar1() << "rownum" << quotechar2()
           << ";" << std::endl
           << std::endl;

    stream << "DROP TABLE IF EXISTS " << schema() << quotechar1()
           << grouped_table << quotechar2() << ";" << std::endl
           << std::endl;

    stream << "DROP TABLE IF EXISTS " << schema() << quotechar1()
           << splitted_table << quotechar2() << ";" << std::endl
           << std::endl;

    return stream.str();
  };

  const auto join_other = [this, &mapping_col, &table_name,
                           &orig_col]() -> std::string {
    std::stringstream stream;
    stream << "UPDATE " << schema() << quotechar1() << table_name
           << quotechar2() << std::endl
           << "SET " << quotechar1() << mapping_col << quotechar2() << " = t2."
           << quotechar1() << "value" << quotechar2() << std::endl
           << "FROM " << schema() << quotechar1()
           << SQLGenerator::to_upper(mapping_col) << quotechar2() << " AS t2"
           << std::endl
           << "WHERE " << quotechar1() << table_name << quotechar2() << "."
           << quotechar1() << orig_col << quotechar2() << " = t2."
           << quotechar1() << "key" << quotechar2() << ";" << std::endl
           << std::endl;
    return stream.str();
  };

  const auto alter_table = [this, &_colname, &table_name]() -> std::string {
    std::stringstream stream;
    stream << "ALTER TABLE " << schema() << quotechar1() << table_name
           << quotechar2() << " ADD " << quotechar1()
           << SQLGenerator::to_lower(_colname) << quotechar2()
           << " DOUBLE PRECISION;" << std::endl
           << std::endl;
    return stream.str();
  };

  const auto set_to_zero = [this, &table_name, &mapping_col]() -> std::string {
    std::stringstream stream;
    stream << "UPDATE " << schema() << quotechar1() << table_name
           << quotechar2() << " SET " << quotechar1() << mapping_col
           << quotechar2() << " = 0.0;" << std::endl
           << std::endl;
    return stream.str();
  };

  const auto drop_table = [this, &_colname]() -> std::string {
    std::stringstream stream;
    stream << drop_table_if_exists(SQLGenerator::to_upper(_colname))
           << std::endl;
    return stream.str();
  };

  if (_is_text && !is_text_field) {
    return alter_table() + set_to_zero() + join_text() + drop_table();
  }

  return alter_table() + set_to_zero() + join_other() + drop_table();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_joins(
    const std::string& _output_name, const std::string& _input_name,
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name) const {
  const auto output_name = SQLGenerator::make_staging_table_name(_output_name);

  const auto input_name = SQLGenerator::make_staging_table_name(_input_name);

  std::stringstream sql;

  sql << "FROM " << schema() << quotechar1() << output_name << quotechar2()
      << " t1" << std::endl;

  sql << "INNER JOIN " << schema() << quotechar1() << input_name << quotechar2()
      << " t2" << std::endl;

  if (_output_join_keys_name == helpers::Macros::no_join_key() ||
      _output_join_keys_name == helpers::Macros::self_join_key()) {
    assert_true(_output_join_keys_name == _input_join_keys_name);

    sql << "ON 1 = 1" << std::endl;
  } else {
    assert_true(_input_join_keys_name != helpers::Macros::no_join_key() &&
                _input_join_keys_name != helpers::Macros::self_join_key());

    sql << SQLGenerator::handle_multiple_join_keys(
        _output_join_keys_name, _input_join_keys_name, "t1", "t2",
        SQLGenerator::NOT_FOR_STAGING, this);
  }

  return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> TSQLGenerator::make_staging_columns(
    const bool& _include_targets, const helpers::Schema& _schema) const {
  const auto cast_column = [this](const std::string& _colname,
                                  const std::string& _coltype,
                                  const bool _replace) -> std::string {
    const auto edited = make_staging_table_column(_colname, "t1");
    const auto replaced =
        _replace ? replace_separators("LOWER( " + edited + " )") : edited;
    std::stringstream stream;
    stream << "CAST( " << replaced << " AS " << _coltype << " ) AS "
           << quotechar1()
           << SQLGenerator::to_lower(make_staging_table_colname(_colname))
           << quotechar2();
    return stream.str();
  };

  const auto is_rowid = [](const std::string& _colname) -> bool {
    return _colname.find(helpers::Macros::rowid()) != std::string::npos;
  };

  const auto is_not_rowid = [&is_rowid](const std::string& _colname) -> bool {
    return !is_rowid(_colname);
  };

  const auto to_epoch_time_or_rowid =
      [this, &is_rowid](const std::string& _colname) -> std::string {
    const auto epoch_time =
        is_rowid(_colname)
            ? make_staging_table_column(_colname, "t1")
            : "datediff( s, '1970-01-01', " +
                  make_staging_table_column(_colname, "t1") + " )";
    std::stringstream stream;
    stream << "CAST( " << epoch_time << " AS DOUBLE PRECISION ) AS "
           << quotechar1()
           << SQLGenerator::to_lower(make_staging_table_colname(_colname))
           << quotechar2();
    return stream.str();
  };

  const auto cast_as_categorical =
      [is_not_rowid, cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast =
        std::bind(cast_column, std::placeholders::_1, "VARCHAR(max)", false);

    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::filter(is_not_rowid) | VIEWS::transform(cast));
  };

  const auto cast_as_join_key =
      [is_not_rowid, cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast =
        std::bind(cast_column, std::placeholders::_1, "VARCHAR(100)", false);

    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::filter(is_not_rowid) | VIEWS::transform(cast));
  };

  const auto cast_as_real =
      [cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast = std::bind(cast_column, std::placeholders::_1,
                                "DOUBLE PRECISION", false);

    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::transform(cast));
  };

  const auto cast_as_time_stamp =
      [to_epoch_time_or_rowid](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::transform(to_epoch_time_or_rowid));
  };

  const auto cast_as_text =
      [is_not_rowid, cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast =
        std::bind(cast_column, std::placeholders::_1, "VARCHAR(max)", true);

    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::filter(is_not_rowid) | VIEWS::transform(cast));
  };

  const auto categoricals = cast_as_categorical(_schema.categoricals_);

  const auto discretes = cast_as_real(_schema.discretes_);

  const auto join_keys = cast_as_join_key(_schema.join_keys_);

  const auto numericals = cast_as_real(_schema.numericals_);

  const auto targets = _include_targets ? cast_as_real(_schema.targets_)
                                        : std::vector<std::string>();

  const auto text = cast_as_text(_schema.text_);

  const auto time_stamps = cast_as_time_stamp(_schema.time_stamps_);

  return stl::join::vector<std::string>({targets, categoricals, discretes,
                                         join_keys, numericals, text,
                                         time_stamps});
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_feature_table(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical,
    const std::string& _prefix) const {
  const auto main_table = SQLGenerator::make_staging_table_name(_main_table);

  std::stringstream stream;
  stream << make_select(_main_table, _autofeatures, _targets, _categorical,
                        _numerical)
         << "INTO " << schema() << quotechar1() << "FEATURES" << _prefix
         << quotechar2() << std::endl
         << "FROM " << schema() << quotechar1() << main_table << quotechar2()
         << " t1" << std::endl
         << "ORDER BY t1." << rowid() << ";" << std::endl
         << std::endl
         << make_updates(_autofeatures, _prefix);
  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_kurtosis_aggregation(
    const std::string& _value) const {
  const auto x = _value;
  const auto m = "AVG( " + x + " )";
  const auto v = "VARP( " + x + " )";
  const auto x_4 = "AVG( POWER( " + x + ", 4 ) )";
  const auto x_3_m = "4 * AVG( POWER( " + x + ", 3 ) ) * " + m;
  const auto x_m_3 = "4 * AVG( " + x + ") * POWER( " + m + ", 3 )";
  const auto x_2_m_2 =
      "6 * AVG( POWER( " + x + ", 2 ) ) * POWER( " + m + ", 2 )";
  const auto m_4 = "POWER( " + m + ", 4 )";
  const auto var_2 = "POWER( " + v + ", 2 )";

  return " /* kurtosis( " + x + " ) */ CASE WHEN " + v +
         " = 0.0 THEN 0.0 ELSE ( " + x_4 + " - " + x_3_m + " + " + x_2_m_2 +
         " - " + x_m_3 + " + " + m_4 + " ) / " + var_2 + " END";
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_skewness_aggregation(
    const std::string& _value) const {
  const auto x = _value;
  const auto m = "AVG( " + x + " )";
  const auto v = "VARP( " + x + " )";
  const auto x_3 = "AVG( POWER( " + x + ", 3 ) )";
  const auto x_2_m = "3 * AVG( POWER( " + x + ", 2 ) ) * " + m;
  const auto x_m_2 = "3 * AVG( " + x + " ) * POWER( " + m + ", 2 )";
  const auto m_3 = "POWER(" + m + ", 3 )";
  const auto var_15 = "POWER( " + v + ", 1.5 )";
  const auto var_is_zero = v + " = 0.0";

  return " /* skewness( " + x + " ) */ CASE WHEN " + var_is_zero +
         " THEN 0.0 ELSE ( " + x_3 + " - " + x_2_m + " + " + x_m_2 + " - " +
         m_3 + " ) / " + var_15 + " END";
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_trend_aggregation(
    const std::string& _value, const std::string& _timestamp) const {
  const auto mean_x = "AVG( CASE WHEN ( " + _value + " ) IS NOT NULL THEN " +
                      _timestamp + " ELSE NULL END )";

  const auto mean_y = "AVG( CASE WHEN ( " + _timestamp +
                      " ) IS NOT NULL THEN " + _value + " ELSE NULL END )";

  const auto count_xy = "COUNT( ( " + _timestamp + " ) + ( " + _value + " ) )";

  const auto sum_xx = "( SUM( ( " + _timestamp + " ) * ( " + _timestamp +
                      " ) ) - " + mean_x + " * " + mean_x + " * " + count_xy +
                      " )";

  const auto sum_xy = "( SUM( ( " + _timestamp + " ) * ( " + _value +
                      " ) ) - " + mean_x + " * " + mean_y + " * " + count_xy +
                      " )";

  const auto beta = sum_xy + " / " + sum_xx;

  return " /* calculate linear trend and extrapolate */ CASE WHEN " + sum_xx +
         " > 0 THEN " + mean_y + " - " + beta + " * " + mean_x + " ELSE AVG( " +
         _value + " ) END";
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_mapping_table_header(
    const std::string& _name, const bool _key_is_num) const {
  const auto quote1 = quotechar1();

  const auto quote2 = quotechar2();

  std::stringstream sql;

  sql << "DROP TABLE IF EXISTS " << schema() << quote1 << _name << quote2 << ";"
      << std::endl
      << std::endl;

  const std::string key_type = _key_is_num ? "INTEGER" : "VARCHAR(max)";

  sql << "CREATE TABLE " << schema() << quote1 << _name << quote2 << "( "
      << quote1 << "key" << quote2 << " " << key_type
      << ", value DOUBLE PRECISION);" << std::endl
      << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_mapping_table_insert_into(
    const std::string& _name) const {
  const auto quote1 = quotechar1();

  const auto quote2 = quotechar2();

  std::stringstream sql;

  sql << "INSERT INTO " << schema() << quote1 << _name << quote2 << " ("
      << quote1 << "key" << quote2 << ", " << quote1 << "value" << quote2 << ")"
      << std::endl
      << "VALUES";

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_postprocessing(
    const std::vector<std::string>& _sql) const {
  std::string sql;

  for (const auto& feature : _sql) {
    const auto pos = feature.find("];\n");

    throw_unless(pos != std::string::npos,
                 "Could not find end of DROP TABLE IF EXISTS statement.");

    sql += feature.substr(0, pos) + "];\n";
  }

  return sql;
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_select(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical) const {
  const auto manual =
      stl::join::vector<std::string>({_targets, _numerical, _categorical});

  const auto make_staging_table_colname_lambda =
      [this](const std::string& _colname) -> std::string {
    return make_staging_table_colname(_colname);
  };

  const auto modified_colnames = helpers::Macros::modify_colnames(
      manual, make_staging_table_colname_lambda);

  std::string sql = "SELECT t1." + rowid() + " AS " + rowid() + ",\n";

  for (size_t i = 0; i < _autofeatures.size(); ++i) {
    const std::string begin = "       ";

    const bool no_comma = (i == _autofeatures.size() - 1 && manual.size() == 0);

    const auto end = (no_comma ? "\n" : ",\n");

    sql += begin + "CAST( 0.0 AS DOUBLE PRECISION ) AS " + quotechar1() +
           _autofeatures.at(i) + quotechar2() + end;
  }

  for (size_t i = 0; i < manual.size(); ++i) {
    const std::string begin = "       ";

    const auto edited_colname =
        "t1." + quotechar1() + modified_colnames.at(i) + quotechar2();

    const std::string data_type =
        (i < _targets.size() + _numerical.size() ? "DOUBLE PRECISION"
                                                 : "VARCHAR(max)");

    const bool no_comma = (i == manual.size() - 1);

    const auto end = no_comma ? "\n" : ",\n";

    sql += begin + "CAST( " + edited_colname + " AS " + data_type + " ) AS " +
           quotechar1() + modified_colnames.at(i) + quotechar2() + end;
  }

  return sql;
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_sql(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _sql,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical) const {
  auto sql = _sql;

  sql.push_back(make_feature_table(_main_table, _autofeatures, _targets,
                                   _categorical, _numerical, ""));

  sql.push_back(make_postprocessing(_sql));

  return stl::collect::string(sql);
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_order_by(const helpers::Schema& _schema) const {
  const auto all_columns = stl::join::vector<std::string>(
      {_schema.join_keys_, _schema.time_stamps_, _schema.categoricals_,
       _schema.discretes_, _schema.numericals_, _schema.text_});

  const auto include = [](const std::string& _colname) -> bool {
    return _colname.find(helpers::Macros::generated_ts()) ==
               std::string::npos &&
           _colname.find(helpers::Macros::rowid()) == std::string::npos &&
           SQLGenerator::include_column(_colname);
  };

  const auto to_colname = [this](const std::string& _colname) {
    return make_staging_table_column(_colname, "t1");
  };

  const auto relevant_columns = stl::collect::vector<std::string>(
      all_columns | VIEWS::filter(include) | VIEWS::transform(to_colname));

  const auto format = [](const size_t _i, const std::string& _colname) {
    const auto begin = _i == 0 ? std::string() : std::string(34, ' ');
    return begin + _colname;
  };

  const auto apply = [&format, &relevant_columns](const size_t _i) {
    return format(_i, relevant_columns[_i]);
  };

  const auto iota = stl::iota<size_t>(0, relevant_columns.size());

  const auto formatted = iota | VIEWS::transform(apply);

  return stl::join::string(formatted, ",\n");
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_percentile_aggregation(
    const std::string& _colname1, const std::string& _q) const {
  return "PERCENTILE_CONT( " + _q + " ) WITHIN GROUP ( ORDER BY " + _colname1 +
         " ) OVER ( PARTITION BY t1." + rowid() + " )";
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_staging_table(
    const bool& _include_targets, const helpers::Schema& _schema) const {
  const auto columns = make_staging_columns(_include_targets, _schema);

  const auto name = SQLGenerator::make_staging_table_name(_schema.name_);

  std::stringstream sql;

  sql << drop_table_if_exists(SQLGenerator::to_upper(name));

  if (schema_ != "") {
    sql << "IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N'"
        << schema_ << "' ) " << std::endl
        << "    EXEC('CREATE SCHEMA " << quotechar1() << schema_ << quotechar2()
        << "');" << std::endl
        << std::endl;
  }

  const auto order_by = make_order_by(_schema);

  sql << "SELECT ROW_NUMBER() OVER( ORDER BY " << order_by << ") AS " << rowid()
      << "," << std::endl;

  for (size_t i = 0; i < columns.size(); ++i) {
    const auto begin = "       ";
    const auto end = (i == columns.size() - 1) ? "" : ",";
    sql << begin << columns.at(i) << end << std::endl;
  }

  sql << "INTO " << schema() << quotechar1() << SQLGenerator::to_upper(name)
      << quotechar2() << std::endl;

  sql << "FROM " << schema() << quotechar1()
      << SQLGenerator::get_table_name(_schema.name_) << quotechar2() << " t1"
      << std::endl;

  sql << SQLGenerator::handle_many_to_one_joins(_schema.name_, "t1", this);

  sql << ";" << std::endl << std::endl;

  sql << create_indices(name, _schema);

  sql << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> TSQLGenerator::make_staging_tables(
    const bool _population_needs_targets,
    const std::vector<bool>& _peripheral_needs_targets,
    const helpers::Schema& _population_schema,
    const std::vector<helpers::Schema>& _peripheral_schema) const {
  auto sql = std::vector<std::string>(
      {make_staging_table(_population_needs_targets, _population_schema)});

  assert_true(_peripheral_schema.size() == _peripheral_needs_targets.size());

  for (size_t i = 0; i < _peripheral_schema.size(); ++i) {
    const auto& schema = _peripheral_schema.at(i);

    auto s = make_staging_table(_peripheral_needs_targets.at(i), schema);

    sql.emplace_back(std::move(s));
  }

  return sql;
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_subfeature_joins(
    const std::string& _feature_prefix, const size_t _peripheral_used,
    const std::string& _alias, const std::string& _feature_postfix) const {
  assert_msg(_alias == "t1" || _alias == "t2", "_alias: " + _alias);

  assert_true(_feature_prefix.size() > 0);

  std::stringstream sql;

  const auto number =
      (_alias == "t2") ? SQLGenerator::make_subfeature_identifier(
                             _feature_prefix, _peripheral_used)
                       : _feature_prefix.substr(0, _feature_prefix.size() - 1);

  const auto letter = _feature_postfix == "" ? 'f' : 'p';

  sql << "LEFT JOIN " << quotechar1() << "FEATURES_" << number
      << _feature_postfix << quotechar2() << " " << letter << "_" << number
      << std::endl;

  sql << "ON " << _alias << "." << rowid() << " = " << letter << "_" << number
      << "." << rowid() << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_time_stamps(
    const std::string& _time_stamp_name,
    const std::string& _lower_time_stamp_name,
    const std::string& _upper_time_stamp_name, const std::string& _output_alias,
    const std::string& _input_alias, const std::string& _t1_or_t2) const {
  std::stringstream sql;

  const auto make_ts_name = [this](const std::string& _raw_name,
                                   const std::string& _alias) {
    const auto colname = make_staging_table_colname(_raw_name);
    return _alias + "." + quotechar1() + colname + quotechar2();
  };

  const auto colname1 = make_ts_name(_time_stamp_name, _output_alias);

  const auto colname2 = make_ts_name(_lower_time_stamp_name, _input_alias);

  sql << colname2 << " <= " << colname1 << std::endl;

  if (_upper_time_stamp_name != "") {
    const auto colname3 = make_ts_name(_upper_time_stamp_name, _input_alias);

    sql << "AND ( " << colname3 << " > " << colname1 << " OR " << colname3
        << " IS NULL )" << std::endl;
  }

  return helpers::StringReplacer::replace_all(
      sql.str(), helpers::Macros::t1_or_t2(), _t1_or_t2);
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::make_updates(
    const std::vector<std::string>& _autofeatures,
    const std::string& _prefix) const {
  std::stringstream stream;

  for (const auto& colname : _autofeatures) {
    const auto table =
        helpers::StringReplacer::replace_all(colname, "feature", "FEATURE");

    stream << "UPDATE " << schema() << quotechar1() << "FEATURES" << _prefix
           << quotechar2() << std::endl
           << "SET " << quotechar1() << colname << quotechar2()
           << " = COALESCE( t2." << quotechar1() << colname << quotechar2()
           << ", 0.0 )" << std::endl
           << "FROM " << schema() << quotechar1() << table << quotechar2()
           << " AS t2" << std::endl
           << "WHERE " << quotechar1() << "FEATURES" << _prefix << quotechar2()
           << "." << rowid() << "= t2." << rowid() << ";" << std::endl
           << std::endl;
  }

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::replace_separators(const std::string& _col) const {
  const auto replace = [this](const std::string _str,
                              const char _c) -> std::string {
    if (_c == ' ') {
      return _str;
    }

    const auto escape_char = handle_escape_char(_c);

    if (escape_char == "") {
      return _str;
    }

    return "REPLACE( " + _str + ", '" + escape_char + "', ' ' )";
  };

  const auto separators = std::string(textmining::StringSplitter::separators_);

  auto str = _col;

  for (const auto c : separators) {
    str = replace(str, c);
  }

  return str;
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::split_text_fields(
    const std::shared_ptr<helpers::ColumnDescription>& _desc,
    const bool _for_mapping) const {
  assert_true(_desc);

  const auto staging_table = SQLGenerator::to_upper(
      transpilation::SQLGenerator::make_staging_table_name(_desc->table_));

  const auto colname =
      SQLGenerator::to_lower(make_staging_table_colname(_desc->name_));

  const auto new_table = staging_table + "__" + SQLGenerator::to_upper(colname);

  std::stringstream stream;

  stream << drop_table_if_exists(new_table) << "SELECT t1." << rowid() << " AS "
         << quotechar1() << "rownum" << quotechar2() << ", value AS "
         << quotechar1() << colname << quotechar2() << std::endl
         << "INTO " << schema() << quotechar1() << new_table << quotechar2()
         << std::endl
         << "FROM " << schema() << quotechar1() << staging_table << quotechar2()
         << " t1" << std::endl
         << "CROSS APPLY STRING_SPLIT( " << colname << ", ' ')" << std::endl
         << "WHERE LEN( value ) > 0;" << std::endl
         << std::endl;

  if (!_for_mapping) {
    stream << std::endl;
  }

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string TSQLGenerator::string_contains(const std::string& _colname,
                                           const std::string& _keyword,
                                           const bool _contains) const {
  const std::string comparison = _contains ? " LIKE " : " NOT LIKE ";
  return "(" + _colname + comparison + "'%" + _keyword + "%' )" + ")";
}

// ----------------------------------------------------------------------------
}  // namespace transpilation
