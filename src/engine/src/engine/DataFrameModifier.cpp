// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/pipelines/DataFrameModifier.hpp"

namespace engine {
namespace pipelines {

void DataFrameModifier::add_join_keys(
    const Poco::JSON::Object& _population_placeholder,
    const std::vector<std::string>& _peripheral_names,
    const std::optional<std::string>& _temp_dir,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs,
    std::shared_ptr<containers::Encoding> _encoding) {
  const auto pool = _encoding || !_temp_dir
                        ? std::shared_ptr<memmap::Pool>()
                        : std::make_shared<memmap::Pool>(*_temp_dir);

  const auto encoding =
      _encoding ? _encoding : std::make_shared<containers::Encoding>(pool);

  const auto joined_tables_arr =
      _population_placeholder.getArray("joined_tables_");

  if (!joined_tables_arr) {
    throw std::runtime_error(
        "The placeholder has no array named 'joined_tables_'!");
  }

  const auto expected_size = joined_tables_arr->size();

  const auto join_keys_used = extract_vector<std::string>(
      _population_placeholder, "join_keys_used_", expected_size);

  const auto other_join_keys_used = extract_vector<std::string>(
      _population_placeholder, "other_join_keys_used_", expected_size);

  for (size_t i = 0; i < join_keys_used.size(); ++i) {
    const auto ptr = joined_tables_arr->getObject(i);

    if (!ptr) {
      throw std::runtime_error("Element " + std::to_string(i) +
                               " of 'joined_tables_' is not a proper JSON "
                               "object!");
    }

    if (join_keys_used.at(i) == helpers::Macros::no_join_key()) {
      auto peripheral_df =
          find_data_frame(*ptr, _peripheral_names, _peripheral_dfs);

      add_jk(peripheral_df);

      add_jk(_population_df);
    } else if (join_keys_used.at(i).find(
                   helpers::Macros::multiple_join_key_sep()) !=
               std::string::npos) {
      auto peripheral_df =
          find_data_frame(*ptr, _peripheral_names, _peripheral_dfs);

      concat_join_keys(other_join_keys_used.at(i), encoding, peripheral_df);

      concat_join_keys(join_keys_used.at(i), encoding, _population_df);
    }

    auto new_population =
        find_data_frame(*ptr, _peripheral_names, _peripheral_dfs);

    add_join_keys(*ptr, _peripheral_names, _temp_dir, new_population,
                  _peripheral_dfs, encoding);
  }
}

// ----------------------------------------------------------------------------

void DataFrameModifier::add_jk(containers::DataFrame* _df) {
  if (_df->has_join_key(helpers::Macros::no_join_key())) {
    return;
  }

  auto new_jk = containers::Column<Int>(_df->pool(), _df->nrows());

  new_jk.set_name(helpers::Macros::no_join_key());

  _df->add_int_column(new_jk, containers::DataFrame::ROLE_JOIN_KEY);
}

// ----------------------------------------------------------------------------

void DataFrameModifier::add_rowid(containers::DataFrame* _df) {
  if (_df->has_time_stamp(helpers::Macros::rowid())) {
    return;
  }

  auto new_ts = containers::Column<Float>(_df->pool(), _df->nrows());

  new_ts.set_name(helpers::Macros::rowid());

  new_ts.set_unit(helpers::Macros::rowid_comparison_only());

  std::iota(new_ts.begin(), new_ts.end(), static_cast<Float>(0.0));

  _df->add_float_column(new_ts, containers::DataFrame::ROLE_TIME_STAMP);
}

// ----------------------------------------------------------------------------

void DataFrameModifier::add_time_stamps(
    const Poco::JSON::Object& _population_placeholder,
    const std::vector<std::string>& _peripheral_names,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs) {
  if (_peripheral_names.size() != _peripheral_dfs->size()) {
    throw std::runtime_error(
        "There must be one peripheral table for every peripheral "
        "placeholder (" +
        std::to_string(_peripheral_dfs->size()) + " vs. " +
        std::to_string(_peripheral_names.size()) + ").");
  }

  const auto joined_tables_arr =
      _population_placeholder.getArray("joined_tables_");

  if (!joined_tables_arr) {
    throw std::runtime_error(
        "The placeholder has no array named 'joined_tables_'!");
  }

  const auto expected_size = joined_tables_arr->size();

  const auto time_stamps_used = extract_vector<std::string>(
      _population_placeholder, "time_stamps_used_", expected_size);

  const auto other_time_stamps_used = extract_vector<std::string>(
      _population_placeholder, "other_time_stamps_used_", expected_size);

  const auto upper_time_stamps_used = extract_vector<std::string>(
      _population_placeholder, "upper_time_stamps_used_", expected_size);

  const auto horizon =
      extract_vector<Float>(_population_placeholder, "horizon_", expected_size);

  const auto memory =
      extract_vector<Float>(_population_placeholder, "memory_", expected_size);

  for (unsigned int i = 0; i < static_cast<unsigned int>(memory.size()); ++i) {
    const auto ptr = joined_tables_arr->getObject(i);

    if (!ptr) {
      throw std::runtime_error(
          "Element " + std::to_string(i) +
          " in 'joined_tables_' is not a proper JSON object!");
    }

    auto df = find_data_frame(*ptr, _peripheral_names, _peripheral_dfs);

    if (time_stamps_used.at(i) == helpers::Macros::rowid()) {
      add_rowid(_population_df);
    }

    if (other_time_stamps_used.at(i) == helpers::Macros::rowid()) {
      add_rowid(df);
    }

    add_ts(*ptr, other_time_stamps_used.at(i), upper_time_stamps_used.at(i),
           horizon.at(i), memory.at(i), _peripheral_names, _peripheral_dfs);

    add_time_stamps(*ptr, _peripheral_names, df, _peripheral_dfs);
  }
}

// ----------------------------------------------------------------------------

void DataFrameModifier::add_ts(
    const Poco::JSON::Object& _joined_table, const std::string& _ts_used,
    const std::string& _upper_ts_used, const Float _horizon,
    const Float _memory, const std::vector<std::string>& _peripheral_names,
    std::vector<containers::DataFrame>* _peripheral_dfs) {
  if (_memory > 0.0 && _upper_ts_used != "") {
    throw std::runtime_error(
        "You can either set an upper time stamp or memory, but not "
        "both!");
  }

  if (_ts_used == "" && _horizon != 0.0) {
    throw std::runtime_error(
        "If the horizon is non-zero, you must pass a time stamp to the "
        ".join(...) method in the placeholder!");
  }

  if (_ts_used == "" && _memory > 0.0) {
    throw std::runtime_error(
        "If the memory is non-zero, you must pass a time stamp to the "
        ".join(...) method in the placeholder!");
  }

  auto df = find_data_frame(_joined_table, _peripheral_names, _peripheral_dfs);

  auto cols = make_time_stamps(_ts_used, _horizon, _memory, *df);

  assert_true(cols.size() == 0 || cols.size() == 1 || cols.size() == 2);

  assert_true(_horizon != 0.0 || _memory > 0.0 || cols.size() == 0);

  assert_true(_horizon == 0.0 || _memory <= 0.0 || cols.size() == 2);

  if (_horizon != 0.0) {
    assert_true(cols.size() > 0);

    cols.at(0).set_name(make_ts_name(_ts_used, _horizon));
  }

  if (_memory > 0.0) {
    assert_true(cols.size() > 0);

    cols.back().set_name(make_ts_name(_ts_used, _horizon + _memory));
  }

  for (auto& col : cols) {
    df->add_float_column(col, containers::DataFrame::ROLE_TIME_STAMP);
  }
}

// ----------------------------------------------------------------------------

void DataFrameModifier::concat_join_keys(
    const std::string& _name,
    const std::shared_ptr<containers::Encoding> _encoding,
    containers::DataFrame* _df) {
  assert_true(_encoding);

  if (_df->has_join_key(_name)) {
    return;
  }

  const auto old_join_keys = get_old_join_keys(_name, *_df);

  auto new_join_key = containers::Column<Int>(_df->pool(), _df->nrows());

  new_join_key.set_name(_name);

  for (size_t i = 0; i < _df->nrows(); ++i) {
    std::string str;

    for (const auto& jk : old_join_keys) {
      const auto val = jk[i];

      if (val < 0) {
        new_join_key[i] = -1;
        break;
      }

      str += std::to_string(val) + "-";
    }

    if (new_join_key[i] != -1) {
      new_join_key[i] = (*_encoding)[str];
    }
  }

  _df->add_int_column(new_join_key, containers::DataFrame::ROLE_JOIN_KEY);
}

// ----------------------------------------------------------------------------

containers::DataFrame* DataFrameModifier::find_data_frame(
    const Poco::JSON::Object& _joined_table,
    const std::vector<std::string>& _peripheral_names,
    std::vector<containers::DataFrame>* _peripheral_dfs) {
  assert_true(_peripheral_names.size() == _peripheral_dfs->size());

  const auto name = JSON::get_value<std::string>(_joined_table, "name_");

  for (size_t i = 0; i < _peripheral_names.size(); ++i) {
    if (_peripheral_names.at(i) == name) {
      return &(_peripheral_dfs->at(i));
    }
  }

  throw std::runtime_error("Placeholder named '" + name +
                           "' not among the peripheral tables.");

  return nullptr;
}

// ----------------------------------------------------------------------------

std::vector<containers::Column<Int>> DataFrameModifier::get_old_join_keys(
    const std::string& _name, const containers::DataFrame& _df) {
  const auto jk_names = helpers::Macros::parse_join_key_name(_name);

  auto old_join_keys = std::vector<containers::Column<Int>>();

  for (const auto& jk : jk_names) {
    old_join_keys.push_back(_df.join_key(jk));
  }

  return old_join_keys;
}

// ----------------------------------------------------------------------------

std::vector<containers::Column<Float>> DataFrameModifier::make_time_stamps(
    const std::string& _ts_name, const Float _horizon, const Float _memory,
    const containers::DataFrame& _df) {
  if (_ts_name == "") {
    return std::vector<containers::Column<Float>>();
  }

  if (_df.num_time_stamps() == 0) {
    throw std::runtime_error("DataFrame '" + _df.name() +
                             "' has no time stamps!");
  }

  const auto horizon_op = [_horizon](const Float val) {
    return val + _horizon;
  };

  const auto mem_op = [_horizon, _memory](const Float val) {
    return val + _horizon + _memory;
  };

  const auto ts = _df.time_stamp(_ts_name);

  std::vector<containers::Column<Float>> cols;

  if (_horizon != 0.0) {
    cols.emplace_back(containers::Column<Float>(_df.pool(), _df.nrows()));

    cols.back().set_unit(ts.unit());

    std::transform(ts.begin(), ts.end(), cols.back().begin(), horizon_op);
  }

  if (_memory > 0.0) {
    cols.emplace_back(containers::Column<Float>(_df.pool(), _df.nrows()));

    cols.back().set_unit(ts.unit());

    std::transform(ts.begin(), ts.end(), cols.back().begin(), mem_op);
  }

  return cols;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
