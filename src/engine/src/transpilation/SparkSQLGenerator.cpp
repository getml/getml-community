#include "transpilation/SparkSQLGenerator.hpp"

// ----------------------------------------------------------------------------

#include "helpers/Macros.hpp"
#include "helpers/StringReplacer.hpp"
#include "stl/stl.hpp"
#include "textmining/textmining.hpp"

// ----------------------------------------------------------------------------

#include "transpilation/SQLGenerator.hpp"

// ----------------------------------------------------------------------------

namespace transpilation {

std::string SparkSQLGenerator::aggregation(
    const helpers::enums::Aggregation& _agg, const std::string& _colname1,
    const std::optional<std::string>& _colname2) const {
  switch (_agg) {
    case helpers::enums::Aggregation::avg_time_between:
      assert_true(_colname2);
      return avg_time_between_aggregation(_colname1, _colname2.value());

    case helpers::enums::Aggregation::count_above_mean:
      return count_above_below_mean_aggregation(_colname1, true);

    case helpers::enums::Aggregation::count_below_mean:
      return count_above_below_mean_aggregation(_colname1, false);

    case helpers::enums::Aggregation::count_distinct:
      return "COUNT( DISTINCT " + _colname1 + " )";

    case helpers::enums::Aggregation::count_distinct_over_count:
      return "CASE WHEN COUNT( " + _colname1 +
             ") == 0 THEN 0 ELSE COUNT( DISTINCT " + _colname1 +
             " ) / COUNT( " + _colname1 + " ) END";

    case helpers::enums::Aggregation::count_minus_count_distinct:
      return "COUNT( " + _colname1 + " ) - COUNT( DISTINCT " + _colname1 + " )";

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
      return "KURTOSIS(" + _colname1 + " ) + 3.0";

    case helpers::enums::Aggregation::last:
      assert_true(_colname2);
      return first_last_aggregation(_colname1, _colname2.value(), false);

    case helpers::enums::Aggregation::median:
      return "PERCENTILE( " + _colname1 + ", 0.5 )";

    case helpers::enums::Aggregation::mode:
      return mode_aggregation(_colname1);

    case helpers::enums::Aggregation::num_max:
      return num_max_min_aggregation(_colname1, true);

    case helpers::enums::Aggregation::num_min:
      return num_max_min_aggregation(_colname1, false);

    case helpers::enums::Aggregation::q1:
      return "PERCENTILE( " + _colname1 + ", 0.01 )";

    case helpers::enums::Aggregation::q5:
      return "PERCENTILE( " + _colname1 + ", 0.05 )";

    case helpers::enums::Aggregation::q10:
      return "PERCENTILE( " + _colname1 + ", 0.1 )";

    case helpers::enums::Aggregation::q25:
      return "PERCENTILE( " + _colname1 + ", 0.25 )";

    case helpers::enums::Aggregation::q75:
      return "PERCENTILE( " + _colname1 + ", 0.75 )";

    case helpers::enums::Aggregation::q90:
      return "PERCENTILE( " + _colname1 + ", 0.9 )";

    case helpers::enums::Aggregation::q95:
      return "PERCENTILE( " + _colname1 + ", 0.95 )";

    case helpers::enums::Aggregation::q99:
      return "PERCENTILE( " + _colname1 + ", 0.99 )";

    case helpers::enums::Aggregation::skew:
      return "SKEWNESS( " + _colname1 + " )";

    case helpers::enums::Aggregation::stddev:
      return "STDDEV_POP( " + _colname1 + " )";

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

    case helpers::enums::Aggregation::var:
      return "VAR_POP( " + _colname1 + " )";

    case helpers::enums::Aggregation::variation_coefficient:
      return "CASE WHEN AVG( " + _colname1 + " ) != 0 THEN VAR_POP( " +
             _colname1 + " ) / AVG( " + _colname1 + " ) ELSE NULL END";

    default:
      const auto agg_type =
          helpers::enums::Parser<helpers::enums::Aggregation>::to_str(_agg);
      return helpers::StringReplacer::replace_all(agg_type, " ", "_") + "( " +
             _colname1 + " )";
  }
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::avg_time_between_aggregation(
    const std::string& _colname1, const std::string& _colname2) const {
  return "CASE WHEN COUNT( * ) > 1 THEN ( MAX( " + _colname2 + " ) - MIN ( " +
         _colname2 + " ) ) / ( COUNT( * ) - 1 )  ELSE 0 END";
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::count_above_below_mean_aggregation(
    const std::string& _colname1, const bool _above) const {
  const auto collect_list = "COLLECT_LIST( float( " + _colname1 + " ) )";

  constexpr auto init =
      "named_struct(\"vals\", array(float(NULL)), \"sum\", float(0.0), "
      "\"count\", float(0.0))";

  constexpr auto update_struct =
      "(s, value) -> ( CASE WHEN value IS NOT NULL THEN named_struct( "
      "\"vals\", concat( s.vals, array(value) ), \"sum\", s.sum + value, "
      "\"count\", s.count + float( 1.0 ) ) ELSE s END )";

  const std::string op = (_above ? ">" : "<");

  const auto count =
      "s -> CASE WHEN s.count > 0.0 THEN float( size( "
      "filter( s.vals, v -> v " +
      op + " ( s.sum / s.count ) ) ) ) ELSE NULL END";

  const std::string comment = _above ? "COUNT_ABOVE_MEAN" : "COUNT_BELOW_MEAN";

  return "/* " + comment + "( " + _colname1 + " ) */ AGGREGATE( " +
         collect_list + ", " + init + ", " + update_struct + ", " + count +
         " )";
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::create_table(
    const helpers::enums::Aggregation& _agg, const std::string& _feature_prefix,
    const std::string& _feature_num) const {
  std::stringstream sql;
  sql << "CREATE TABLE " << quotechar1() << "FEATURE_" << _feature_prefix
      << _feature_num << quotechar2() << " AS" << std::endl;
  return sql.str();
}

// ----------------------------------------------------------------------------

std::tuple<std::string, std::string, std::string>
SparkSQLGenerator::demangle_colname(const std::string& _raw_name) const {
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
      "substr( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::begin(), helpers::Macros::postfix() + ", ");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::length(), helpers::Macros::postfix() + ", ");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::hour_begin(),
      "lpad( string( hour( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::hour_end(),
      helpers::Macros::postfix() + ") ), 2, '0' )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::minute_begin(),
      "lpad( string( minute( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::minute_end(),
      helpers::Macros::postfix() + ") ), 2, '0' )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::month_begin(),
      "date_format( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::month_end(),
      helpers::Macros::postfix() + ", \"MM\" ) /* month */");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::weekday_begin(),
      "dayofweek( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::weekday_end(),
      helpers::Macros::postfix() + " ) - 1");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::year_begin(),
      "date_format( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::year_end(),
      helpers::Macros::postfix() + ", \"yyyy\" ) /* year */");

  const auto pos1 = new_name.rfind(helpers::Macros::prefix()) +
                    helpers::Macros::prefix().size();

  const auto pos2 = new_name.find(helpers::Macros::postfix());

  throw_unless(pos2 >= pos1,
               "Error: helpers::Macros in colname do not make sense!");

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

  const auto edited_postfix = [&_raw_name, &postfix]() -> std::string {
    if (_raw_name.find(helpers::Macros::diffstr()) == std::string::npos) {
      return postfix;
    }

    if (_raw_name.find(helpers::Macros::rowid()) != std::string::npos) {
      return postfix;
    }

    const auto interval =
        " + INTERVAL " +
        std::to_string(SQLGenerator::parse_time_stamp_diff(postfix)) +
        " seconds";

    const auto pos = postfix.find(" )");

    if (pos == std::string::npos) {
      return interval;
    }

    return interval + postfix.substr(pos);
  };

  return std::make_tuple(prefix, new_name, edited_postfix());
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::drop_batch_tables(
    const std::vector<std::string>& _autofeatures,
    const std::string& _prefix) const {
  const auto num_batches = _autofeatures.size() % BATCH_SIZE == 0
                               ? _autofeatures.size() / BATCH_SIZE
                               : _autofeatures.size() / BATCH_SIZE + 1;

  const auto iota = stl::iota<size_t>(0, num_batches);

  const auto drop_table = [&_prefix](size_t _i) -> std::string {
    std::stringstream sql;

    sql << "DROP TABLE IF EXISTS `FEATURES" << _prefix << "_BATCH_"
        << std::to_string(_i + 1) << "`;" << std::endl;

    return sql.str();
  };

  return stl::collect::string(iota | VIEWS::transform(drop_table));
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::drop_table_if_exists(
    const std::string& _table_name) const {
  std::stringstream sql;
  sql << "DROP TABLE IF EXISTS " << quotechar1() << _table_name << quotechar2()
      << ";" << std::endl
      << std::endl;
  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_staging_table_column(
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

  const auto quotation =
      (_raw_name.find(helpers::Macros::rowid()) != std::string::npos ||
       alias == "")
          ? ""
          : "`";

  const auto only_alphanumeric =
      SQLGenerator::replace_non_alphanumeric(new_name);

  return prefix + alias + dot + quotation + only_alphanumeric + quotation +
         postfix;
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::first_last_aggregation(
    const std::string& _colname1, const std::string& _colname2,
    const bool _first) const {
  const std::string collect_list =
      "COLLECT_LIST( named_struct( \"value\", float( " + _colname1 +
      " ), \"ts\", float( " + _colname2 + " ) ) )";

  const auto array_sort = "ARRAY_SORT( " + collect_list +
                          ", (left, right) -> CASE "
                          "WHEN left.ts < right.ts THEN -1 "
                          "WHEN left.ts > right.ts THEN 1 "
                          "ELSE 0 END )";

  const std::string first_or_last = _first ? "FIRST" : "LAST";

  const std::string index = _first ? "1" : "-1";

  return "/* " + first_or_last + "*/ ELEMENT_AT( " + array_sort + ", " + index +
         " ).value";
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::first_or_last_optimum_aggregation(
    const std::string& _colname1, const std::string& _colname2,
    const bool _is_first, const bool _is_minimum) const {
  const std::string ts_compare = _is_first ? ">" : "<";

  const std::string value_compare = _is_minimum ? "<" : ">";

  const std::string zip_with = "ZIP_WITH( COLLECT_LIST( float( " + _colname1 +
                               " ) ), COLLECT_LIST( float( " + _colname2 +
                               " ) ), (value, ts) -> (value, ts) )";

  constexpr auto init =
      "named_struct( \"value\", float(NULL), \"ts\", float(NULL) )";

  const std::string fold =
      "(struct1, struct2) -> ( CASE WHEN struct1.value IS NULL OR "
      "struct1.ts IS NULL THEN struct2 WHEN struct2.value " +
      value_compare +
      " struct1.value THEN struct2 WHEN struct2.value = struct1.value "
      "AND struct2.ts " +
      ts_compare + " struct1.ts THEN struct2 ELSE struct1 END )";

  constexpr auto extract = "struct -> struct.ts";

  const std::string first_or_last = _is_first ? "FIRST" : "LAST";

  const std::string minimum_or_maximum = _is_minimum ? "MINIMUM" : "MAXIMUM";

  const auto comment = "TIME_SINCE_" + first_or_last + "_" + minimum_or_maximum;

  return "/* " + comment + " */ AGGREGATE( " + zip_with + ", " + init + ", " +
         fold + ", " + extract + " )";
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::join_batch_tables(
    const std::vector<std::string>& _autofeatures,
    const std::string& _prefix) const {
  const auto num_batches = _autofeatures.size() % BATCH_SIZE == 0
                               ? _autofeatures.size() / BATCH_SIZE
                               : _autofeatures.size() / BATCH_SIZE + 1;

  const auto iota = stl::iota<size_t>(0, num_batches);

  const auto join_table = [&_prefix](size_t _i) -> std::string {
    std::stringstream sql;

    sql << "LEFT JOIN `FEATURES" << _prefix << "_BATCH_"
        << std::to_string(_i + 1) << "` b" << _i + 1 << std::endl
        << "ON t1.rowid = b" << _i + 1 << ".`rownum`" << std::endl;

    return sql.str();
  };

  return stl::collect::string(iota | VIEWS::transform(join_table));
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::join_mapping(const std::string& _name,
                                            const std::string& _colname,
                                            const bool _is_text) const {
  const bool is_text_field =
      (_name.find(helpers::Macros::text_field()) != std::string::npos);

  const auto table_name =
      SQLGenerator::to_upper(SQLGenerator::make_staging_table_name(_name));

  const auto temp_table_name = table_name + "__TEMP";

  const auto mapping_col = SQLGenerator::to_lower(_colname);

  const auto pos = mapping_col.find("__mapping_");

  assert_true(pos != std::string::npos);

  const auto orig_col = mapping_col.substr(0, pos);

  const auto alter_tables = [&temp_table_name, &table_name]() -> std::string {
    std::stringstream sql;

    sql << "DROP TABLE IF EXISTS `" << temp_table_name << "`;" << std::endl
        << std::endl
        << "ALTER TABLE `" << table_name << "` RENAME TO `" << temp_table_name
        << "`;" << std::endl
        << std::endl;

    return sql.str();
  };

  const auto join_text = [&table_name, &temp_table_name, &mapping_col,
                          &orig_col]() -> std::string {
    constexpr auto avg_value =
        "AVG( COALESCE( t3.`value`, 0.0 ) ) AS `avg_value`";

    const auto mapping_table = SQLGenerator::to_upper(mapping_col);

    const auto split = "SPLIT( " + orig_col + ", '[ ]' )";

    const auto contains = "ARRAY_CONTAINS( " + split + ", t3.`key` )";

    std::stringstream sql;

    sql << "CREATE TABLE `" << table_name << "`" << std::endl
        << "SELECT t1.*, COALESCE( t2.`avg_value`, 0.0 ) AS `" << mapping_col
        << "`" << std::endl
        << "FROM `" << temp_table_name << "` t1" << std::endl
        << "LEFT JOIN (" << std::endl
        << "    SELECT t4.`" << orig_col << "`, " << avg_value << std::endl
        << "    FROM `" << temp_table_name << "` t4" << std::endl
        << "    LEFT JOIN `" << mapping_table << "` t3" << std::endl
        << "    ON " << contains << std::endl
        << "    GROUP BY t4.`" << orig_col << "`" << std::endl
        << ") AS t2" << std::endl
        << "ON t1.`" << orig_col << "` = t2.`" << orig_col << "`;" << std::endl
        << std::endl;

    return sql.str();
  };

  const auto join_other = [&table_name, &temp_table_name, &mapping_col,
                           &orig_col]() -> std::string {
    std::stringstream sql;

    sql << "CREATE TABLE `" << table_name << "`" << std::endl
        << "SELECT t1.*, COALESCE( t2.`value`, 0.0 ) AS `" << mapping_col << "`"
        << std::endl
        << "FROM `" << temp_table_name << "` t1" << std::endl
        << "LEFT JOIN `" << SQLGenerator::to_upper(mapping_col) << "` t2"
        << std::endl
        << "ON t1.`" << orig_col << "` = t2.key;" << std::endl
        << std::endl;

    return sql.str();
  };

  const auto drop_tables = [&temp_table_name, &mapping_col]() -> std::string {
    std::stringstream sql;

    sql << "DROP TABLE IF EXISTS `" << temp_table_name << "`;" << std::endl
        << std::endl
        << "DROP TABLE IF EXISTS `" << SQLGenerator::to_upper(mapping_col)
        << "`;" << std::endl
        << std::endl
        << std::endl;

    return sql.str();
  };

  const auto join = (_is_text && !is_text_field) ? join_text() : join_other();

  std::stringstream sql;

  sql << alter_tables() << join << drop_tables();

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_ewma_aggregation(
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

    return "/* exponentially weighted moving average */ CASE WHEN COUNT( " +
           _value + " ) > 0 THEN SUM( ( " + _value + " ) * " + exp +
           " ) / SUM( " + exp + " ) ELSE NULL END";
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

std::string SparkSQLGenerator::make_trend_aggregation(
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

std::string SparkSQLGenerator::make_batch_tables(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::string& _prefix) const {
  const auto num_batches = _autofeatures.size() % BATCH_SIZE == 0
                               ? _autofeatures.size() / BATCH_SIZE
                               : _autofeatures.size() / BATCH_SIZE + 1;

  const auto iota = stl::iota<size_t>(0, num_batches);

  const auto make_table = [this, &_main_table, &_autofeatures,
                           &_prefix](size_t _i) -> std::string {
    const auto batch_begin = _i * BATCH_SIZE;

    const auto range =
        _autofeatures | VIEWS::drop(batch_begin) | VIEWS::take(BATCH_SIZE);

    const auto autofeatures = stl::collect::vector<std::string>(range);

    std::stringstream sql;

    sql << make_feature_table(_main_table, autofeatures, {}, {}, {},
                              _prefix + "_BATCH_" + std::to_string(_i + 1))
        << std::endl;

    return sql.str();
  };

  return stl::collect::string(iota | VIEWS::transform(make_table));
}
// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_staging_table_colname(
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

  const auto final_name =
      alias + underscore + prefix + SQLGenerator::to_lower(new_name) + postfix;

  return SQLGenerator::replace_non_alphanumeric(final_name);
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_joins(
    const std::string& _output_name, const std::string& _input_name,
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name) const {
  const auto output_name = SQLGenerator::make_staging_table_name(_output_name);

  const auto input_name = SQLGenerator::make_staging_table_name(_input_name);

  std::stringstream sql;

  sql << "FROM `" << output_name << "` t1" << std::endl;

  sql << "INNER JOIN `" << input_name << "` t2" << std::endl;

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

std::string SparkSQLGenerator::make_mapping_table_header(
    const std::string& _name, const bool _key_is_num) const {
  const auto quote1 = quotechar1();

  const auto quote2 = quotechar2();

  std::stringstream sql;

  sql << "DROP TABLE IF EXISTS " << quote1 << _name << quote2 << ";"
      << std::endl
      << std::endl;

  const std::string key_type = _key_is_num ? "DOUBLE" : "STRING";

  sql << "CREATE TABLE " << quote1 << _name << quote2 << "(key " << key_type
      << " NOT NULL, value DOUBLE);" << std::endl
      << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_mapping_table_insert_into(
    const std::string& _name) const {
  const auto quote1 = quotechar1();

  const auto quote2 = quotechar2();

  std::stringstream sql;

  sql << "INSERT INTO " << quote1 << _name << quote2 << " (key, value)"
      << std::endl
      << "VALUES";

  return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> SparkSQLGenerator::make_staging_columns(
    const bool& _include_targets, const helpers::Schema& _schema) const {
  const auto cast_column = [this](const std::string& _colname,
                                  const std::string& _coltype,
                                  const bool _replace) -> std::string {
    const auto edited = make_staging_table_column(_colname, "t1");
    const auto replaced = _replace ? replace_separators(edited) : edited;
    return "CAST( " + replaced + " AS " + _coltype + " ) AS `" +
           SQLGenerator::to_lower(make_staging_table_colname(_colname)) + "`";
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
            : "to_timestamp( " + make_staging_table_column(_colname, "t1") +
                  " )";

    return "CAST( " + epoch_time + " AS DOUBLE ) AS `" +
           SQLGenerator::to_lower(make_staging_table_colname(_colname)) + "`";
  };

  const auto cast_as_real =
      [cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast =
        std::bind(cast_column, std::placeholders::_1, "DOUBLE", false);

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
      [cast_column, &is_not_rowid](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast =
        std::bind(cast_column, std::placeholders::_1, "STRING", false);

    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::filter(is_not_rowid) | VIEWS::transform(cast));
  };

  const auto replace_seps =
      [is_not_rowid, cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast =
        std::bind(cast_column, std::placeholders::_1, "STRING", true);

    return stl::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::filter(is_not_rowid) | VIEWS::transform(cast));
  };

  const auto categoricals = cast_as_text(_schema.categoricals_);

  const auto discretes = cast_as_real(_schema.discretes_);

  const auto join_keys = cast_as_text(_schema.join_keys_);

  const auto numericals = cast_as_real(_schema.numericals_);

  const auto targets = _include_targets ? cast_as_real(_schema.targets_)
                                        : std::vector<std::string>();

  const auto text = replace_seps(_schema.text_);

  const auto time_stamps = cast_as_time_stamp(_schema.time_stamps_);

  return stl::join::vector<std::string>({targets, categoricals, discretes,
                                         join_keys, numericals, text,
                                         time_stamps});
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_feature_joins(
    const std::vector<std::string>& _autofeatures) const {
  std::stringstream sql;

  for (const auto& colname : _autofeatures) {
    const auto alias =
        helpers::StringReplacer::replace_all(colname, "feature_", "f_");

    sql << "LEFT JOIN `" << SQLGenerator::to_upper(colname) << "` " << alias
        << std::endl
        << "ON t1.rowid = " << alias << ".`rownum`" << std::endl;
  }

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_feature_table(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical,
    const std::string& _prefix) const {
  const auto main_table = SQLGenerator::make_staging_table_name(_main_table);

  if (_autofeatures.size() <= BATCH_SIZE) {
    std::stringstream sql;

    sql << "DROP TABLE IF EXISTS `FEATURES" << _prefix << "`;" << std::endl
        << std::endl
        << "CREATE TABLE `FEATURES" << _prefix << "` AS" << std::endl
        << make_select(_main_table, _autofeatures, _targets, _categorical,
                       _numerical)
        << "FROM `" << main_table << "` t1" << std::endl
        << make_feature_joins(_autofeatures) << ";" << std::endl
        << std::endl;

    return sql.str();
  }

  std::stringstream sql;

  sql << make_batch_tables(_main_table, _autofeatures, _prefix)
      << "DROP TABLE IF EXISTS `FEATURES" << _prefix << "`;" << std::endl
      << std::endl
      << "CREATE TABLE `FEATURES" << _prefix << "` AS" << std::endl
      << make_select(_main_table, _autofeatures, _targets, _categorical,
                     _numerical)
      << "FROM `" << main_table << "` t1" << std::endl
      << join_batch_tables(_autofeatures, _prefix) << ";" << std::endl
      << std::endl
      << drop_batch_tables(_autofeatures, _prefix);

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_postprocessing(
    const std::vector<std::string>& _sql) const {
  std::stringstream sql;

  for (const auto& feature : _sql) {
    const auto pos = feature.find("`;");

    throw_unless(pos != std::string::npos,
                 "Could not find end of DROP TABLE IF EXISTS statement.");

    sql << feature.substr(0, pos) << "`;" << std::endl;
  }

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_select(
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

  std::stringstream sql;

  if (manual.size() > 0) {
    sql << "SELECT ";
  } else {
    sql << "SELECT t1.rowid AS `rownum`," << std::endl;
  }

  for (size_t i = 0; i < _autofeatures.size(); ++i) {
    const std::string begin = (i == 0 && manual.size() > 0 ? "" : "       ");

    const bool no_comma = (i == _autofeatures.size() - 1 && manual.size() == 0);

    const auto end = (no_comma ? "" : ",");

    const auto batch_num = (i / BATCH_SIZE) + 1;

    const auto alias = _autofeatures.size() > BATCH_SIZE
                           ? "b" + std::to_string(batch_num)
                           : helpers::StringReplacer::replace_all(
                                 _autofeatures.at(i), "feature_", "f_");

    sql << begin << "CAST( COALESCE( " << alias << ".`" << _autofeatures.at(i)
        << "`, 0.0 ) AS DOUBLE ) AS `" << _autofeatures.at(i) << "`" << end
        << std::endl;
  }

  for (size_t i = 0; i < manual.size(); ++i) {
    const std::string begin = "       ";

    const auto edited_colname = "t1.`" + modified_colnames.at(i) + "`";

    const std::string data_type =
        (i < _targets.size() + _numerical.size() ? "DOUBLE" : "STRING");

    const bool no_comma = (i == manual.size() - 1);

    const auto end = no_comma ? "`" : "`,";

    sql << begin << "CAST( " << edited_colname << " AS " << data_type
        << " ) AS `" << modified_colnames.at(i) << end << std::endl;
  }

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::handle_escape_char(const char c) const {
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
      return "\\'";

    case ';':
    case '[':
    case ']':
      return "";

    default:
      return std::string(1, c);
  }
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_separators() const {
  const auto handle = [this](const char c) -> std::string {
    return handle_escape_char(c);
  };

  const auto separators = std::string(textmining::StringSplitter::separators_);

  return stl::collect::string(separators | VIEWS::transform(handle));
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_sql(
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

std::string SparkSQLGenerator::make_staging_table(
    const bool& _include_targets, const helpers::Schema& _schema) const {
  const auto columns = make_staging_columns(_include_targets, _schema);

  const auto name = SQLGenerator::make_staging_table_name(_schema.name_);

  const auto gap = [](const size_t _i) -> std::string {
    return (_i == 0 ? "" : "       ");
  };

  std::stringstream sql;

  sql << "DROP TABLE IF EXISTS `" << SQLGenerator::to_upper(name) << "`;"
      << std::endl
      << std::endl;

  sql << "CREATE TABLE `" << SQLGenerator::to_upper(name) << "` AS" << std::endl
      << "SELECT ";

  for (size_t i = 0; i < columns.size(); ++i) {
    sql << gap(i) << columns.at(i) << "," << std::endl;
  }

  sql << gap(columns.size()) << "monotonically_increasing_id() AS `rowid`"
      << std::endl;

  sql << "FROM `" << SQLGenerator::get_table_name(_schema.name_) << "` t1"
      << std::endl;

  sql << SQLGenerator::handle_many_to_one_joins(_schema.name_, "t1", this);

  sql << ";" << std::endl << std::endl;

  sql << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> SparkSQLGenerator::make_staging_tables(
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

std::string SparkSQLGenerator::make_subfeature_joins(
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

  sql << "LEFT JOIN `FEATURES_" << number << _feature_postfix << "` " << letter
      << "_" << number << std::endl;

  sql << "ON " << _alias << ".rowid = " << letter << "_" << number
      << ".`rownum`" << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::make_time_stamps(
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

std::string SparkSQLGenerator::mode_aggregation(
    const std::string& _colname1) const {
  const auto collect_list = "COLLECT_LIST( float( " + _colname1 + " ) )";

  constexpr auto init = "map(float(0.0), 0)";

  constexpr auto update_map =
      "(m, key) -> (CASE WHEN ( size( map_filter( m, (k, v) -> k = key ) "
      ") = 0 ) "
      "THEN map_concat( m, map(key, 1) ) ELSE transform_values( m, "
      "(k, v) -> CASE WHEN k = key THEN v + 1 ELSE v END ) END )";

  constexpr auto take_max_element =
      "m -> element_at( array_sort( map_entries(m), (left, right) -> "
      "CASE WHEN left.value > right.value THEN -1 WHEN left.value < "
      "right.value THEN 1 WHEN left.key < right.key THEN -1 ELSE 1 END "
      "), 1).key";

  return "/* MODE( " + _colname1 + " ) */ AGGREGATE( " + collect_list + ", " +
         init + ", " + update_map + ", " + take_max_element + " )";
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::num_max_min_aggregation(
    const std::string& _colname1, const bool _max) const {
  const std::string optimum = _max ? "max_value" : "min_value";

  const std::string op = _max ? ">" : "<";

  const auto collect_list = "COLLECT_LIST( float( " + _colname1 + " ) )";

  const std::string init =
      "named_struct(\"count\", 0, \"" + optimum + "\", float(NULL))";

  const auto update_struct = "(s, value) -> ( CASE WHEN s." + optimum +
                             " IS NULL OR value " + op + " s." + optimum +
                             " THEN named_struct( \"count\", 1, \"" + optimum +
                             "\", value) WHEN value = s." + optimum +
                             " THEN named_struct( \"count\", s.count + 1, \"" +
                             optimum + "\", value ) ELSE s END )";

  constexpr auto count =
      "s -> ( CASE WHEN s.count > 0 THEN float( s.count ) ELSE NULL END "
      ")";

  const std::string comment = _max ? "NUM_MAX" : "NUM_MIN";

  return "/* " + comment + "( " + _colname1 + " ) */ AGGREGATE( " +
         collect_list + ", " + init + ", " + update_struct + ", " + count +
         " )";
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::replace_separators(
    const std::string& _col) const {
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

std::string SparkSQLGenerator::split_text_fields(
    const std::shared_ptr<helpers::ColumnDescription>& _desc,
    const bool _for_mapping) const {
  assert_true(_desc);

  const auto staging_table =
      transpilation::SQLGenerator::make_staging_table_name(_desc->table_);

  const auto colname = make_staging_table_colname(_desc->name_);

  const auto new_table = staging_table + "__" + SQLGenerator::to_upper(colname);

  const auto split = "SPLIT( t1.`" + colname + "`, '[ ]' )";

  const auto filter = "FILTER( " + split + ", word -> word != \"\" )";

  const auto transform = "TRANSFORM( " + filter +
                         ", word -> named_struct( \"rownum\", t1.rowid, \"" +
                         colname + "\", LOWER( word ) ) )";

  std::stringstream sql;

  sql << "DROP TABLE IF EXISTS `" + new_table + "`;" << std::endl
      << std::endl
      << "CREATE TABLE `" << new_table << "` AS " << std::endl
      << "SELECT INLINE( " << transform << "  )" << std::endl
      << "FROM " << staging_table << " t1;" << std::endl
      << std::endl
      << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string SparkSQLGenerator::string_contains(const std::string& _colname,
                                               const std::string& _keyword,
                                               const bool _contains) const {
  const auto split = "SPLIT( " + _colname + ", '[ ]' )";

  const std::string not_or_nothing = _contains ? "" : "! ";

  return not_or_nothing + "ARRAY_CONTAINS( " + split + ", '" + _keyword + "' )";
}

// ----------------------------------------------------------------------------
}  // namespace transpilation
