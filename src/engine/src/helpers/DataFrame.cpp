// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/DataFrame.hpp"

#include <cstddef>
#include <range/v3/view/concat.hpp>
#include <vector>

#include "helpers/Macros.hpp"

namespace helpers {

DataFrame::DataFrame(const DataFrameParams& _params)
    : categoricals_(_params.categoricals_),
      discretes_(_params.discretes_),
      indices_(_params.indices_),
      join_keys_(_params.join_keys_),
      name_(_params.name_),
      numericals_(_params.numericals_),
      row_indices_(_params.row_indices_),
      targets_(_params.targets_),
      text_(_params.text_),
      time_stamps_(_params.time_stamps_),
      ts_index_(_params.ts_index_),
      word_indices_(_params.word_indices_) {
  assert_true(_params.indices_.size() == _params.join_keys_.size());

  assert_true(_params.row_indices_.size() == 0 ||
              _params.row_indices_.size() == _params.text_.size());

  assert_true(_params.word_indices_.size() == 0 ||
              _params.word_indices_.size() == _params.text_.size());

#ifndef NDEBUG
  for (auto& col : _params.categoricals_) {
    assert_msg(col.nrows_ == nrows(),
               "categoricals: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _params.discretes_) {
    assert_msg(col.nrows_ == nrows(),
               "discretes: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _params.join_keys_) {
    assert_msg(col.nrows_ == nrows(),
               "join_keys: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _params.numericals_) {
    assert_msg(col.nrows_ == nrows(),
               "numericals: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _params.targets_) {
    assert_msg(col.nrows_ == nrows(),
               "targets: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _params.text_) {
    assert_msg(col.nrows_ == nrows(),
               "text: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _params.time_stamps_) {
    assert_msg(col.nrows_ == nrows(),
               "time_stamps: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }
#endif  // NDEBUG
}

DataFrame DataFrame::create_subview(const CreateSubviewParams& _params) const {
  const auto ix_join_key =
      find_ix_join_key(_params.join_key_, _params.make_staging_table_colname_);

  const auto numericals_and_time_stamps = make_numericals_and_time_stamps(
      _params.additional_, _params.allow_lagged_targets_,
      _params.upper_time_stamp_);

  const auto df_params =
      DataFrameParams{.categoricals_ = categoricals_,
                      .discretes_ = discretes_,
                      .indices_ = {indices_.at(ix_join_key)},
                      .join_keys_ = {join_keys_.at(ix_join_key)},
                      .name_ = name_,
                      .numericals_ = numericals_and_time_stamps,
                      .row_indices_ = _params.row_indices_,
                      .targets_ = targets_,
                      .text_ = text_,
                      .time_stamps_ = {},
                      .word_indices_ = _params.word_indices_};

  if (_params.time_stamp_ == "") {
    return DataFrame(df_params);
  }

  const auto ix_time_stamp = find_ix_time_stamp(
      _params.time_stamp_, _params.make_staging_table_colname_);

  if (_params.upper_time_stamp_ == "") {
    return DataFrame(
        df_params.with_time_stamps({time_stamps_.at(ix_time_stamp)}));
  }

  const auto ts_index = make_ts_index(_params, ix_join_key, ix_time_stamp);

  const auto ix_upper_time_stamp = find_ix_time_stamp(
      _params.upper_time_stamp_, _params.make_staging_table_colname_);

  return DataFrame(df_params
                       .with_time_stamps({time_stamps_.at(ix_time_stamp),
                                          time_stamps_.at(ix_upper_time_stamp)})
                       .with_ts_index(ts_index));
}

// ----------------------------------------------------------------------------

std::pair<const size_t*, const size_t*> DataFrame::find(
    const Int _join_key, const size_t _ix_join_key) const {
  assert_true(indices().size() > _ix_join_key);

  assert_true(indices_[_ix_join_key]);

  if (std::holds_alternative<InMemoryIndex>(*indices_[_ix_join_key])) {
    const auto& idx = std::get<InMemoryIndex>(*indices_[_ix_join_key]);

    const auto it = idx.find(_join_key);

    if (it == idx.end()) {
      return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
    }

    return std::make_pair(it->second.data(),
                          it->second.data() + it->second.size());
  }

  if (std::holds_alternative<MemoryMappedIndex>(*indices_[_ix_join_key])) {
    const auto& idx = std::get<MemoryMappedIndex>(*indices_[_ix_join_key]);

    const auto opt = idx[_join_key];

    if (!opt) {
      return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
    }

    const auto begin = opt->data();

    return std::make_pair(begin, begin + opt->size());
  }

  assert_true(false);

  return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
}

// ----------------------------------------------------------------------------

size_t DataFrame::find_ix_join_key(
    const std::string& _colname,
    const std::optional<std::function<std::string(std::string)>>&
        _make_staging_table_colname) const {
  for (size_t ix_join_key = 0; ix_join_key < join_keys_.size(); ++ix_join_key) {
    if (join_keys_[ix_join_key].name_ == _colname) {
      return ix_join_key;
    }
  }

  if (!_make_staging_table_colname) {
    throw std::runtime_error("Join key named '" + _colname +
                             "' not found in table '" + name_ + "'");
  }

  const auto get_name =
      [this, &_make_staging_table_colname](const auto& _col) -> std::string {
    const auto [table, colname] =
        Macros::parse_table_colname(name_, _col.name_);
    return " '" + (*_make_staging_table_colname)(colname) + "',";
  };

  auto names = join_keys_ | std::views::transform(get_name) | std::views::join |
               std::ranges::to<std::string>();

  if (names.size() > 0) {
    names.back() = '.';
  }

  const auto [table, colname] = Macros::parse_table_colname(name_, _colname);

  throw std::runtime_error(
      "Join key named '" + (*_make_staging_table_colname)(colname) +
      "' not found in table '" + table + "'. Found " +
      std::to_string(join_keys_.size()) + " join keys:" + names);

  return 0;
}

// ----------------------------------------------------------------------------

size_t DataFrame::find_ix_time_stamp(
    const std::string& _colname,
    const std::optional<std::function<std::string(std::string)>>&
        _make_staging_table_colname) const {
  for (size_t ix_time_stamp = 0; ix_time_stamp < time_stamps_.size();
       ++ix_time_stamp) {
    if (time_stamps_[ix_time_stamp].name_ == _colname) {
      return ix_time_stamp;
    }
  }

  if (!_make_staging_table_colname) {
    throw std::runtime_error("Time stamp named '" + _colname +
                             "' not found in table '" + name_ + "'");
  }

  const auto get_name =
      [this, &_make_staging_table_colname](const auto& _col) -> std::string {
    const auto [table, colname] =
        Macros::parse_table_colname(name_, _col.name_);
    return (*_make_staging_table_colname)(colname);
  };

  auto names = time_stamps_ | std::views::transform(get_name) |
               std::views::join | std::ranges::to<std::string>();

  if (names.size() > 0) {
    names.back() = '.';
  }

  const auto [table, colname] = Macros::parse_table_colname(name_, _colname);

  throw std::runtime_error(
      "Time stamp named '" + (*_make_staging_table_colname)(colname) +
      "' not found in table '" + table + "'. Found " +
      std::to_string(time_stamps_.size()) + " time stamps:" + names);

  return 0;
}

// ----------------------------------------------------------------------------

std::vector<Column<Float>> DataFrame::make_numericals_and_time_stamps(
    const AdditionalColumns& _additional, const bool _allow_lagged_targets,
    const std::string& _upper_time_stamp) const {
  const auto targets =
      _allow_lagged_targets ? targets_ : std::vector<Column<Float>>();

  const auto is_not_upper =
      [&_upper_time_stamp](const Column<Float>& _ts) -> bool {
    return _upper_time_stamp == "" || _ts.name_ != _upper_time_stamp;
  };

  const auto time_stamps = time_stamps_ | std::views::filter(is_not_upper) |
                           std::ranges::to<std::vector>();

  return ranges::views::concat(numericals_, _additional, targets, time_stamps) |
         std::ranges::to<std::vector>();
}

// ----------------------------------------------------------------------------

std::shared_ptr<tsindex::Index> DataFrame::make_ts_index(
    const CreateSubviewParams& _params, const size_t _ix_join_key,
    const size_t _ix_time_stamp) const {
  if (nrows() == 0) {
    return nullptr;
  }

  if (!_params.population_join_keys_) {
    return nullptr;
  }

  if (_params.population_join_keys_->nrows() == 0) {
    return nullptr;
  }

  const auto ix_upper_time_stamp = find_ix_time_stamp(
      _params.upper_time_stamp_, _params.make_staging_table_colname_);

  const auto upper_ts = time_stamps_.at(ix_upper_time_stamp);

  const auto [_, colname] = Macros::parse_table_colname(name_, upper_ts.name_);

  if (colname.find(Macros::generated_ts()) == std::string::npos) {
    return nullptr;
  }

  const auto join_keys = fct::Range<const Int*>(
      join_keys_.at(_ix_join_key).begin(), join_keys_.at(_ix_join_key).end());

  const auto lower_ts =
      fct::Range<const Float*>(time_stamps_.at(_ix_time_stamp).begin(),
                               time_stamps_.at(_ix_time_stamp).end());

  const auto memory = upper_ts[0] - lower_ts.begin()[0];

  const auto unique_join_keys =
      *_params.population_join_keys_ | std::ranges::to<std::set>();

  const auto find_rownums =
      [this, _ix_join_key](const Int jk) -> fct::Range<const size_t*> {
    const auto p = find(jk, _ix_join_key);
    return fct::Range<const size_t*>(p.first, p.second);
  };

  const auto rownums = std::make_shared<std::vector<std::size_t>>(
      unique_join_keys | std::views::transform(find_rownums) |
      std::views::join | std::ranges::to<std::vector>());

  const auto params = tsindex::IndexParams{.join_keys_ = join_keys,
                                           .lower_ts_ = lower_ts,
                                           .memory_ = memory,
                                           .rownums_ = rownums};

  return std::make_shared<tsindex::Index>(params);
}

// ----------------------------------------------------------------------------
}  // namespace helpers
