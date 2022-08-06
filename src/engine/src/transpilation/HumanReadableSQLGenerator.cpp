// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "transpilation/HumanReadableSQLGenerator.hpp"

// ----------------------------------------------------------------------------

#include "fct/fct.hpp"

// ----------------------------------------------------------------------------

#include "helpers/enums/Aggregation.hpp"
#include "transpilation/SQLGenerator.hpp"

// ----------------------------------------------------------------------------

namespace transpilation {

std::string HumanReadableSQLGenerator::aggregation(
    const helpers::enums::Aggregation& _agg, const std::string& _colname1,
    const std::optional<std::string>& _colname2) const {
  const auto sep =
      _agg == helpers::enums::Aggregation::trend ? ", " : " ORDER BY ";

  const auto value = _colname2 ? _colname1 + sep + *_colname2 : _colname1;

  if (_agg == helpers::enums::Aggregation::count_distinct) {
    return "COUNT( DISTINCT " + value + " )";
  }

  if (_agg == helpers::enums::Aggregation::count_minus_count_distinct) {
    return "COUNT( " + value + "  ) - COUNT( DISTINCT " + value + " )";
  }

  const auto agg_type =
      helpers::enums::Parser<helpers::enums::Aggregation>::to_str(_agg);

  return helpers::StringReplacer::replace_all(agg_type, " ", "_") + "( " +
         value + " )";
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::create_indices(
    const std::string& _table_name, const helpers::Schema& _schema) const {
  const auto create_index =
      [this, &_table_name](const std::string& _colname) -> std::string {
    const auto colname = make_staging_table_colname(_colname);

    const auto index_name = _table_name + "__" + colname;

    const auto drop = "DROP INDEX IF EXISTS \"" + index_name + "\";\n";

    return drop + "CREATE INDEX \"" + index_name + "\" ON \"" + _table_name +
           "\" (\"" + colname + "\");\n\n";
  };

  return fct::collect::string(_schema.join_keys_ |
                              VIEWS::filter(SQLGenerator::include_column) |
                              VIEWS::transform(create_index)) +
         fct::collect::string(_schema.time_stamps_ |
                              VIEWS::transform(create_index));
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::create_table(
    const helpers::enums::Aggregation& _agg, const std::string& _feature_prefix,
    const std::string& _feature_num) const {
  std::stringstream sql;
  sql << "CREATE TABLE " << quotechar1() << "FEATURE_" << _feature_prefix
      << _feature_num << quotechar2() << " AS" << std::endl;
  return sql.str();
}

// ----------------------------------------------------------------------------

std::tuple<std::string, std::string, std::string>
HumanReadableSQLGenerator::demangle_colname(
    const std::string& _raw_name) const {
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
      "hour( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::hour_end(), helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::minute_begin(),
      "minute( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::minute_end(),
      helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::month_begin(),
      "month( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::month_end(),
      helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::weekday_begin(),
      "weekday( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::weekday_end(),
      helpers::Macros::postfix() + " )");

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::year_begin(),
      "year( " + helpers::Macros::prefix());

  new_name = helpers::StringReplacer::replace_all(
      new_name, helpers::Macros::year_end(), helpers::Macros::postfix() + " )");

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

  return std::make_tuple(prefix, new_name, postfix);
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::drop_table_if_exists(
    const std::string& _table_name) const {
  std::stringstream sql;
  sql << "DROP TABLE IF EXISTS " << quotechar1() << _table_name << quotechar2()
      << ";" << std::endl
      << std::endl;
  return sql.str();
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_staging_table_column(
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
          : "\"";

  return prefix + alias + dot + quotation + new_name + quotation + postfix;
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_staging_table_colname(
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

  return alias + underscore + prefix + SQLGenerator::to_lower(new_name) +
         postfix;
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_joins(
    const std::string& _output_name, const std::string& _input_name,
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name) const {
  const auto output_name = SQLGenerator::make_staging_table_name(_output_name);

  const auto input_name = SQLGenerator::make_staging_table_name(_input_name);

  std::stringstream sql;

  sql << "FROM \"" << output_name << "\" t1" << std::endl;

  sql << "INNER JOIN \"" << input_name << "\" t2" << std::endl;

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

std::vector<std::string> HumanReadableSQLGenerator::make_staging_columns(
    const bool& _include_targets, const helpers::Schema& _schema) const {
  const auto cast_column = [this](const std::string& _colname,
                                  const std::string& _coltype) -> std::string {
    return "CAST( " + make_staging_table_column(_colname, "t1") + " AS " +
           _coltype + " ) AS \"" +
           SQLGenerator::to_lower(make_staging_table_colname(_colname)) + "\"";
  };

  const auto to_ts = [this](const std::string& _colname) -> std::string {
    const auto ts = make_staging_table_column(_colname, "t1");
    return "CAST( " + ts + " AS TIMESTAMP ) AS \"" +
           SQLGenerator::to_lower(make_staging_table_colname(_colname)) + "\"";
  };

  const auto cast_as_real =
      [cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast = std::bind(cast_column, std::placeholders::_1, "REAL");

    return fct::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::transform(cast));
  };

  const auto cast_as_time_stamp =
      [to_ts](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    return fct::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::transform(to_ts));
  };

  const auto cast_as_text =
      [cast_column](const std::vector<std::string>& _colnames)
      -> std::vector<std::string> {
    const auto cast = std::bind(cast_column, std::placeholders::_1, "TEXT");

    return fct::collect::vector<std::string>(
        _colnames | VIEWS::filter(SQLGenerator::include_column) |
        VIEWS::transform(cast));
  };

  const auto categoricals = cast_as_text(_schema.categoricals_);

  const auto discretes = cast_as_real(_schema.discretes_);

  const auto join_keys = cast_as_text(_schema.join_keys_);

  const auto numericals = cast_as_real(_schema.numericals_);

  const auto targets = _include_targets ? cast_as_real(_schema.targets_)
                                        : std::vector<std::string>();

  const auto text = cast_as_text(_schema.text_);

  const auto time_stamps = cast_as_time_stamp(_schema.time_stamps_);

  return fct::join::vector<std::string>({targets, categoricals, discretes,
                                         join_keys, numericals, text,
                                         time_stamps});
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_feature_table(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical,
    const std::string& _prefix) const {
  std::string sql = "DROP TABLE IF EXISTS \"FEATURES" + _prefix + "\";\n\n";

  sql += "CREATE TABLE \"FEATURES" + _prefix + "\" AS\n";

  sql += make_select(_main_table, _autofeatures, _targets, _categorical,
                     _numerical);

  const auto main_table = SQLGenerator::make_staging_table_name(_main_table);

  sql += "FROM \"" + main_table + "\" t1\n";

  sql += "ORDER BY t1.rowid;\n\n";

  sql += make_updates(_autofeatures, _prefix);

  return sql;
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_postprocessing(
    const std::vector<std::string>& _sql) const {
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

std::string HumanReadableSQLGenerator::make_select(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical) const {
  const auto manual =
      fct::join::vector<std::string>({_targets, _numerical, _categorical});

  const auto make_staging_table_colname_lambda =
      [this](const std::string& _colname) -> std::string {
    return make_staging_table_colname(_colname);
  };

  const auto modified_colnames = helpers::Macros::modify_colnames(
      manual, make_staging_table_colname_lambda);

  std::string sql =
      manual.size() > 0 ? "SELECT " : "SELECT t1.rowid AS \"rownum\",\n";

  for (size_t i = 0; i < _autofeatures.size(); ++i) {
    const std::string begin = (i == 0 && manual.size() > 0 ? "" : "       ");

    const bool no_comma = (i == _autofeatures.size() - 1 && manual.size() == 0);

    const auto end = (no_comma ? "\n" : ",\n");

    sql +=
        begin + "CAST( 0.0 AS REAL ) AS \"" + _autofeatures.at(i) + "\"" + end;
  }

  for (size_t i = 0; i < manual.size(); ++i) {
    const std::string begin = "       ";

    const auto edited_colname = "t1.\"" + modified_colnames.at(i) + "\"";

    const std::string data_type =
        (i < _targets.size() + _numerical.size() ? "REAL" : "TEXT");

    const bool no_comma = (i == manual.size() - 1);

    const auto end = no_comma ? "\"\n" : "\",\n";

    sql += begin + "CAST( " + edited_colname + " AS " + data_type + " ) AS \"" +
           modified_colnames.at(i) + end;
  }

  return sql;
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_sql(
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

  return fct::collect::string(sql);
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_staging_table(
    const bool& _include_targets, const helpers::Schema& _schema) const {
  const auto columns = make_staging_columns(_include_targets, _schema);

  const auto name = SQLGenerator::make_staging_table_name(_schema.name_);

  std::stringstream sql;

  sql << "DROP TABLE IF EXISTS \"" << SQLGenerator::to_upper(name) << "\";\n\n";

  sql << "CREATE TABLE \"" << SQLGenerator::to_upper(name) << "\" AS\nSELECT ";

  for (size_t i = 0; i < columns.size(); ++i) {
    const auto begin = (i == 0) ? "" : "       ";
    const auto end = (i == columns.size() - 1) ? "\n" : ",\n";
    sql << begin << columns.at(i) << end;
  }

  sql << "FROM \"" << SQLGenerator::get_table_name(_schema.name_) << "\" t1\n";

  sql << SQLGenerator::handle_many_to_one_joins(_schema.name_, "t1", this);

  sql << ";" << std::endl << std::endl;

  sql << create_indices(name, _schema);

  sql << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> HumanReadableSQLGenerator::make_staging_tables(
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

std::string HumanReadableSQLGenerator::make_subfeature_joins(
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

  sql << "LEFT JOIN \"FEATURES_" << number << _feature_postfix << "\" "
      << letter << "_" << number << std::endl;

  sql << "ON " << _alias << ".rowid = " << letter << "_" << number
      << ".\"rownum\"" << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::make_time_stamps(
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

std::string HumanReadableSQLGenerator::make_updates(
    const std::vector<std::string>& _autofeatures,
    const std::string& _prefix) const {
  std::string sql;

  for (const auto& colname : _autofeatures) {
    const auto table =
        helpers::StringReplacer::replace_all(colname, "feature", "FEATURE");

    sql += "UPDATE \"FEATURES" + _prefix + "\"\n";
    sql +=
        "SET \"" + colname + "\" = COALESCE( t2.\"" + colname + "\", 0.0 )\n";
    sql += "FROM \"" + table + "\" AS t2\n";
    sql += "WHERE \"FEATURES" + _prefix + "\".rowid = t2.\"rownum\";\n\n";
  }

  return sql;
}

// ----------------------------------------------------------------------------

std::string HumanReadableSQLGenerator::split_text_fields(
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

std::string HumanReadableSQLGenerator::string_contains(
    const std::string& _colname, const std::string& _keyword,
    const bool _contains) const {
  const std::string comparison = _contains ? " > 0 " : " == 0 ";

  return "( contains( " + _colname + ", '" + _keyword + "' )" + comparison +
         ")";
}

// ----------------------------------------------------------------------------
}  // namespace transpilation
