// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/Seasonal.hpp"

#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "helpers/Loader.hpp"
#include "helpers/Saver.hpp"

namespace engine {
namespace preprocessors {

std::optional<containers::Column<Int>> Seasonal::extract_hour(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories) const {
  auto result = to_categorical(_col, ADD_ZERO, utils::Time::hour, _categories);

  result.set_name(helpers::Macros::hour_begin() + _col.name() +
                  helpers::Macros::hour_end());
  result.set_unit("hour");

  if (PreprocessorImpl::has_warnings(result)) {
    return std::nullopt;
  }

  return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_hour(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col) const {
  auto result = to_categorical(_categories, _col, ADD_ZERO, utils::Time::hour);

  result.set_name(helpers::Macros::hour_begin() + _col.name() +
                  helpers::Macros::hour_end());
  result.set_unit("hour");

  return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_minute(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories) const {
  auto result =
      to_categorical(_col, ADD_ZERO, utils::Time::minute, _categories);

  result.set_name(helpers::Macros::minute_begin() + _col.name() +
                  helpers::Macros::minute_end());
  result.set_unit("minute");

  if (PreprocessorImpl::has_warnings(result)) {
    return std::nullopt;
  }

  return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_minute(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col) const {
  auto result =
      to_categorical(_categories, _col, ADD_ZERO, utils::Time::minute);

  result.set_name(helpers::Macros::minute_begin() + _col.name() +
                  helpers::Macros::minute_end());
  result.set_unit("minute");

  return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_month(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories) const {
  auto result = to_categorical(_col, ADD_ZERO, utils::Time::month, _categories);

  result.set_name(helpers::Macros::month_begin() + _col.name() +
                  helpers::Macros::month_end());
  result.set_unit("month");

  if (PreprocessorImpl::has_warnings(result)) {
    return std::nullopt;
  }

  return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_month(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col) const {
  auto result = to_categorical(_categories, _col, ADD_ZERO, utils::Time::month);

  result.set_name(helpers::Macros::month_begin() + _col.name() +
                  helpers::Macros::month_end());
  result.set_unit("month");

  return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_weekday(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories) const {
  auto result =
      to_categorical(_col, DONT_ADD_ZERO, utils::Time::weekday, _categories);

  result.set_name(helpers::Macros::weekday_begin() + _col.name() +
                  helpers::Macros::weekday_end());
  result.set_unit("weekday");

  if (PreprocessorImpl::has_warnings(result)) {
    return std::nullopt;
  }

  return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_weekday(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col) const {
  auto result =
      to_categorical(_categories, _col, DONT_ADD_ZERO, utils::Time::weekday);

  result.set_name(helpers::Macros::weekday_begin() + _col.name() +
                  helpers::Macros::weekday_end());
  result.set_unit("weekday");

  return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Float>> Seasonal::extract_year(
    const containers::Column<Float>& _col) {
  auto result = to_numerical(_col, utils::Time::year);

  result.set_name(helpers::Macros::year_begin() + _col.name() +
                  helpers::Macros::year_end());
  result.set_unit("year, comparison only");

  if (PreprocessorImpl::has_warnings(result)) {
    return std::nullopt;
  }

  return result;
}

// ----------------------------------------------------

containers::Column<Float> Seasonal::extract_year(
    const containers::Column<Float>& _col) const {
  auto result = to_numerical(_col, utils::Time::year);

  result.set_name(helpers::Macros::year_begin() + _col.name() +
                  helpers::Macros::year_end());
  result.set_unit("year, comparison only");

  return result;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Seasonal::fit_transform(const FitParams& _params) {
  const auto population_df = fit_transform_df(
      _params.population_df_, helpers::ColumnDescription::POPULATION, 0,
      _params.categories_.get());

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs_.size(); ++i) {
    const auto& df = _params.peripheral_dfs_.at(i);

    const auto new_df =
        fit_transform_df(df, helpers::ColumnDescription::PERIPHERAL, i,
                         _params.categories_.get());

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame Seasonal::fit_transform_df(
    const containers::DataFrame& _df, const std::string& _marker,
    const size_t _table, containers::Encoding* _categories) {
  const auto blacklist = std::vector<helpers::Subrole>(
      {helpers::Subrole::exclude_preprocessors, helpers::Subrole::email_only,
       helpers::Subrole::substring_only, helpers::Subrole::exclude_seasonal});

  auto df = _df;

  for (size_t i = 0; i < _df.num_time_stamps(); ++i) {
    const auto& ts = _df.time_stamp(i);

    if (ts.name().find(helpers::Macros::generated_ts()) != std::string::npos) {
      continue;
    }

    if (helpers::SubroleParser::contains_any(ts.subroles(), blacklist)) {
      continue;
    }

    auto col = extract_hour(ts, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, ts.name(), &hour_);
      df.add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }

    col = extract_minute(ts, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, ts.name(), &minute_);
      df.add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }

    col = extract_month(ts, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, ts.name(), &month_);
      df.add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }

    col = extract_weekday(ts, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, ts.name(), &weekday_);
      df.add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }

    const auto year = extract_year(ts);

    if (year) {
      PreprocessorImpl::add(_marker, _table, ts.name(), &year_);
      df.add_float_column(*year, containers::DataFrame::ROLE_NUMERICAL);
    }
  }

  return df;
}

// ----------------------------------------------------

void Seasonal::load(const std::string& _fname) {
  const auto named_tuple =
      helpers::Loader::load_from_json<NamedTupleType>(_fname);
  hour_ = named_tuple.get<f_hour>();
  minute_ = named_tuple.get<f_minute>();
  month_ = named_tuple.get<f_month>();
  weekday_ = named_tuple.get<f_weekday>();
  year_ = named_tuple.get<f_year>();
}

// ----------------------------------------------------

void Seasonal::save(const std::string& _fname) const {
  helpers::Saver::save_as_json(_fname, *this);
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::to_int(
    const containers::Column<Float>& _col, const bool _add_zero,
    containers::Encoding* _categories) const {
  const auto to_str = [_categories, _add_zero](const Float val) {
    auto str = io::Parser::to_string(val);
    if (_add_zero && str.size() == 1) {
      str = '0' + str;
    }
    return (*_categories)[str];
  };

  auto result = containers::Column<Int>(_col.pool(), _col.nrows());

  std::transform(_col.begin(), _col.end(), result.begin(), to_str);

  return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::to_int(
    const containers::Encoding& _categories, const bool _add_zero,
    const containers::Column<Float>& _col) const {
  const auto to_str = [&_categories, _add_zero](const Float val) {
    auto str = io::Parser::to_string(val);
    if (_add_zero && str.size() == 1) {
      str = '0' + str;
    }
    return _categories[str];
  };

  auto result = containers::Column<Int>(_col.pool(), _col.nrows());

  std::transform(_col.begin(), _col.end(), result.begin(), to_str);

  return result;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Seasonal::transform(const TransformParams& _params) const {
  const auto population_df =
      transform_df(*_params.categories_, _params.population_df_,
                   helpers::ColumnDescription::POPULATION, 0);

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs_.size(); ++i) {
    const auto& df = _params.peripheral_dfs_.at(i);

    const auto new_df = transform_df(*_params.categories_, df,
                                     helpers::ColumnDescription::PERIPHERAL, i);

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame Seasonal::transform_df(
    const containers::Encoding& _categories, const containers::DataFrame& _df,
    const std::string& _marker, const size_t _table) const {
  auto df = _df;

  // ----------------------------------------------------

  auto names = PreprocessorImpl::retrieve_names(_marker, _table, hour_);

  for (const auto& name : names) {
    const auto col = extract_hour(_categories, df.time_stamp(name));

    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  // ----------------------------------------------------

  names = PreprocessorImpl::retrieve_names(_marker, _table, minute_);

  for (const auto& name : names) {
    const auto col = extract_minute(_categories, df.time_stamp(name));

    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  // ----------------------------------------------------

  names = PreprocessorImpl::retrieve_names(_marker, _table, month_);

  for (const auto& name : names) {
    const auto col = extract_month(_categories, df.time_stamp(name));

    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  // ----------------------------------------------------

  names = PreprocessorImpl::retrieve_names(_marker, _table, weekday_);

  for (const auto& name : names) {
    const auto col = extract_weekday(_categories, df.time_stamp(name));

    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  // ----------------------------------------------------

  names = PreprocessorImpl::retrieve_names(_marker, _table, year_);

  for (const auto& name : names) {
    const auto col = extract_year(df.time_stamp(name));

    df.add_float_column(col, containers::DataFrame::ROLE_NUMERICAL);
  }

  // ----------------------------------------------------

  return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
