// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/pipelines/Staging.hpp"

namespace engine {
namespace pipelines {
// ----------------------------------------------------------------------------

containers::Column<Int> Staging::extract_join_key(
    const containers::DataFrame& _df, const std::string& _tname,
    const std::string& _alias, const std::string& _colname) {
  const auto name = helpers::Macros::make_colname(_tname, _alias, _colname);

  if (_df.has_join_key(name)) {
    return _df.join_key(name);
  }

  return _df.join_key(_colname);
}

// ----------------------------------------------------------------------------

containers::DataFrameIndex Staging::extract_index(
    const containers::DataFrame& _df, const std::string& _tname,
    const std::string& _alias, const std::string& _colname) {
  const auto name = helpers::Macros::make_colname(_tname, _alias, _colname);

  if (_df.has_join_key(name)) {
    return _df.index(name);
  }

  return _df.index(_colname);
}

// ----------------------------------------------------------------------------

std::optional<containers::Column<Float>> Staging::extract_time_stamp(
    const containers::DataFrame& _df, const std::string& _tname,
    const std::string& _alias, const std::string& _colname) {
  if (_colname == "") {
    return std::nullopt;
  }

  const auto name = helpers::Macros::make_colname(_tname, _alias, _colname);

  if (_df.has_time_stamp(name)) {
    return _df.time_stamp(name);
  }

  return _df.time_stamp(_colname);
}

// ----------------------------------------------------------------------------

containers::DataFrame Staging::find_peripheral(
    const std::string& _name, const std::vector<std::string>& _peripheral_names,
    const std::vector<containers::DataFrame>& _peripheral_dfs) {
  if (_peripheral_dfs.size() != _peripheral_names.size()) {
    throw std::runtime_error(
        "The number of peripheral tables must match the number of "
        "placeholders passed. This is the point of having "
        "placeholders!");
  }

  for (size_t i = 0; i < _peripheral_names.size(); ++i) {
    if (_peripheral_names.at(i) == _name) {
      return _peripheral_dfs.at(i);
    }
  }

  throw std::runtime_error("Could not find any placeholder named '" + _name +
                           "' among the peripheral placeholders!");

  return _peripheral_dfs.at(0);
}

// ----------------------------------------------------------------------------

containers::DataFrame Staging::join_all(
    const size_t _number, const bool _is_population,
    const std::string& _joined_name,
    const std::vector<std::string>& _origin_peripheral_names,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs) {
  const auto splitted = helpers::StringSplitter::split(
      _joined_name, helpers::Macros::delimiter());

  assert_true(splitted.size() != 0);

  auto population = _population_df;

  if (!_is_population) {
    population = find_peripheral(splitted.at(0), _origin_peripheral_names,
                                 _peripheral_dfs);
  }

  for (size_t i = 1; i < splitted.size(); ++i) {
    population = join_one(splitted.at(i), population, _peripheral_dfs,
                          _origin_peripheral_names);
  }

  population.set_name(_joined_name + helpers::Macros::staging_table_num() +
                      std::to_string(_number));

  return population;
}

// ----------------------------------------------------------------------------

containers::DataFrame Staging::join_one(
    const std::string& _splitted, const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::vector<std::string>& _peripheral_names) {
  auto joined = _population;

  const auto [name, alias, join_key, other_join_key, time_stamp,
              other_time_stamp, upper_time_stamp, joined_to_name,
              joined_to_alias, one_to_one] =
      helpers::Macros::parse_table_name(_splitted);

  const auto peripheral =
      find_peripheral(name, _peripheral_names, _peripheral_dfs);

  const auto index =
      make_index(name, alias, join_key, other_join_key, time_stamp,
                 other_time_stamp, upper_time_stamp, joined_to_name,
                 joined_to_alias, one_to_one, _population, peripheral);

  for (size_t i = 0; i < peripheral.num_categoricals(); ++i) {
    auto col = peripheral.categorical(i).sort_by_key(index);
    col.set_name(helpers::Macros::make_colname(name, alias, col.name()));
    joined.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  for (size_t i = 0; i < peripheral.num_join_keys(); ++i) {
    auto col = peripheral.join_key(i).sort_by_key(index);
    col.set_name(helpers::Macros::make_colname(name, alias, col.name()));
    joined.add_int_column(col, containers::DataFrame::ROLE_JOIN_KEY);
  }

  for (size_t i = 0; i < peripheral.num_numericals(); ++i) {
    auto col = peripheral.numerical(i).sort_by_key(index);
    col.set_name(helpers::Macros::make_colname(name, alias, col.name()));
    joined.add_float_column(col, containers::DataFrame::ROLE_NUMERICAL);
  }

  for (size_t i = 0; i < peripheral.num_text(); ++i) {
    auto col = peripheral.text(i).sort_by_key(index);
    col.set_name(helpers::Macros::make_colname(name, alias, col.name()));
    joined.add_string_column(col, containers::DataFrame::ROLE_TEXT);
  }

  for (size_t i = 0; i < peripheral.num_time_stamps(); ++i) {
    auto col = peripheral.time_stamp(i).sort_by_key(index);
    col.set_name(helpers::Macros::make_colname(name, alias, col.name()));
    joined.add_float_column(col, containers::DataFrame::ROLE_TIME_STAMP);
  }

  for (size_t i = 0; i < peripheral.num_unused_strings(); ++i) {
    if (peripheral.unused_string(i).unit() == "") {
      continue;
    }
    auto col = peripheral.unused_string(i).sort_by_key(index);
    col.set_name(helpers::Macros::make_colname(name, alias, col.name()));
    joined.add_string_column(col, containers::DataFrame::ROLE_UNUSED_STRING);
  }

  return joined;
}

// ----------------------------------------------------------------------------

void Staging::join_tables(
    const std::vector<std::string>& _origin_peripheral_names,
    const std::string& _joined_population_name,
    const std::vector<std::string>& _joined_peripheral_names,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs) {
  const auto population_df =
      join_all(1, true, _joined_population_name, _origin_peripheral_names,
               *_population_df, *_peripheral_dfs);

  auto peripheral_dfs =
      std::vector<containers::DataFrame>(_joined_peripheral_names.size());

  for (size_t i = 0; i < peripheral_dfs.size(); ++i) {
    peripheral_dfs.at(i) =
        join_all(i + 2, false, _joined_peripheral_names.at(i),
                 _origin_peripheral_names, *_population_df, *_peripheral_dfs);
  }

  *_population_df = population_df;

  *_peripheral_dfs = peripheral_dfs;
}

// ----------------------------------------------------------------------------

std::vector<size_t> Staging::make_index(
    const std::string& _name, const std::string& _alias,
    const std::string& _join_key, const std::string& _other_join_key,
    const std::string& _time_stamp, const std::string& _other_time_stamp,
    const std::string& _upper_time_stamp, const std::string& _joined_to_name,
    const std::string& _joined_to_alias, const bool _one_to_one,
    const containers::DataFrame& _population,
    const containers::DataFrame& _peripheral) {
  // -------------------------------------------------------

  const auto join_key = extract_join_key(_population, _joined_to_name,
                                         _joined_to_alias, _join_key);

  const auto peripheral_index =
      extract_index(_peripheral, _name, _alias, _other_join_key);

  const auto time_stamp = extract_time_stamp(_population, _joined_to_name,
                                             _joined_to_alias, _time_stamp);

  const auto other_time_stamp =
      extract_time_stamp(_peripheral, _name, _alias, _other_time_stamp);

  const auto upper_time_stamp =
      extract_time_stamp(_peripheral, _name, _alias, _upper_time_stamp);

  // -------------------------------------------------------

  if ((time_stamp && true) != (other_time_stamp && true)) {
    throw std::runtime_error(
        "If you pass a time stamp, there must also be another time "
        "stamp and vice versa!");
  }

  // -------------------------------------------------------

  std::vector<size_t> index(_population.nrows());

  std::set<size_t> unique_indices;

  for (size_t i = 0; i < _population.nrows(); ++i) {
    const auto ts = time_stamp ? (*time_stamp)[i] : 0.0;

    const auto [ix, ok] =
        retrieve_index(_population.nrows(), join_key[i], ts, peripheral_index,
                       other_time_stamp, upper_time_stamp);

    if (!ok) {
      throw std::runtime_error("The join of '" + _population.name() +
                               "' and '" + _peripheral.name() +
                               "' was marked many-to-one or one-to-one, "
                               "but there is more than one "
                               "match in '" +
                               _peripheral.name() + "'.");
    }

    if (_one_to_one && ix < _population.nrows()) {
      if (unique_indices.find(ix) != unique_indices.end()) {
        throw std::runtime_error("The join of '" + _population.name() +
                                 "' and '" + _peripheral.name() +
                                 "' was marked one-to-one, but there is more "
                                 "than one match in '" +
                                 _population.name() + "'.");
      }

      unique_indices.insert(ix);
    }

    index[i] = ix;
  }

  // -------------------------------------------------------

  return index;

  // -------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<size_t, bool> Staging::retrieve_index(
    const size_t _nrows, const Int _jk, const Float _ts,
    const containers::DataFrameIndex& _peripheral_index,
    const std::optional<containers::Column<Float>>& _other_time_stamp,
    const std::optional<containers::Column<Float>>& _upper_time_stamp) {
  const auto [begin, end] = _peripheral_index.find(_jk);

  if (begin == nullptr) {
    return std::make_pair(_nrows, true);
  }

  std::optional<size_t> local_index = std::nullopt;

  for (auto it = begin; it != end; ++it) {
    const auto ix = *it;

    const auto lower = _other_time_stamp ? (*_other_time_stamp)[ix] : 0.0;

    const auto upper = _upper_time_stamp ? (*_upper_time_stamp)[ix] : NAN;

    const bool match_in_range =
        lower <= _ts && (std::isnan(upper) || upper > _ts);

    if (!match_in_range) {
      continue;
    }

    if (local_index) {
      return std::make_pair(_nrows, false);
    }

    local_index = ix;
  }

  if (!local_index) {
    return std::make_pair(_nrows, true);
  }

  return std::make_pair(*local_index, true);
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
