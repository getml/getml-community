#include "helpers/DataFrame.hpp"

// ----------------------------------------------------------------------------

#include "stl/stl.hpp"

// ----------------------------------------------------------------------------

#include "helpers/Macros.hpp"
#include "helpers/SQLite3Generator.hpp"

// ----------------------------------------------------------------------------
namespace helpers {
// ----------------------------------------------------------------------------

DataFrame::DataFrame(const std::vector<Column<Int>>& _categoricals,
                     const std::vector<Column<Float>>& _discretes,
                     const std::vector<std::shared_ptr<Index>>& _indices,
                     const std::vector<Column<Int>>& _join_keys,
                     const std::string& _name,
                     const std::vector<Column<Float>>& _numericals,
                     const std::vector<Column<Float>>& _targets,
                     const std::vector<Column<strings::String>>& _text,
                     const std::vector<Column<Float>>& _time_stamps,
                     const RowIndices& _row_indices,
                     const WordIndices& _word_indices)
    : categoricals_(_categoricals),
      discretes_(_discretes),
      indices_(_indices),
      join_keys_(_join_keys),
      name_(_name),
      numericals_(_numericals),
      row_indices_(_row_indices),
      targets_(_targets),
      text_(_text),
      time_stamps_(_time_stamps),
      word_indices_(_word_indices) {
  assert_true(_indices.size() == _join_keys.size());

  assert_true(_row_indices.size() == 0 || _row_indices.size() == _text.size());

  assert_true(_word_indices.size() == 0 ||
              _word_indices.size() == _text.size());

#ifndef NDEBUG
  for (auto& col : _categoricals) {
    assert_msg(col.nrows_ == nrows(),
               "categoricals: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _discretes) {
    assert_msg(col.nrows_ == nrows(),
               "discretes: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _join_keys) {
    assert_msg(col.nrows_ == nrows(),
               "join_keys: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _numericals) {
    assert_msg(col.nrows_ == nrows(),
               "numericals: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _targets) {
    assert_msg(col.nrows_ == nrows(),
               "targets: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _text) {
    assert_msg(col.nrows_ == nrows(),
               "text: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }

  for (auto& col : _time_stamps) {
    assert_msg(col.nrows_ == nrows(),
               "time_stamps: col.nrows_: " + std::to_string(col.nrows_) +
                   ", nrows(): " + std::to_string(nrows()));
  }
#endif  // NDEBUG
}

// ----------------------------------------------------------------------------

DataFrame DataFrame::create_subview(
    const std::string& _join_key, const std::string& _time_stamp,
    const std::string& _upper_time_stamp, const bool _allow_lagged_targets,
    const RowIndices& _row_indices, const WordIndices& _word_indices,
    const AdditionalColumns& _additional) const {
  // ---------------------------------------------------------------------------

  const auto ix_join_key = find_ix_join_key(_join_key);

  // ---------------------------------------------------------------------------
  // All time stamps that are not upper time stamp are added to numerical
  // and given the unit time stamp - this is so the end users do not have
  // to understand the difference between time stamps as a type and
  // time stamps as a role.

  auto numericals_and_time_stamps = std::vector<Column<Float>>();

  for (const auto& col : numericals_) {
    numericals_and_time_stamps.push_back(col);
  }

  for (const auto& col : _additional) {
    numericals_and_time_stamps.push_back(col);
  }

  if (_allow_lagged_targets) {
    for (const auto& col : targets_) {
      numericals_and_time_stamps.push_back(col);
    }
  }

  for (const auto& col : time_stamps_) {
    if (_upper_time_stamp != "" && col.name_ == _upper_time_stamp) {
      continue;
    }

    const auto ts =
        Column<Float>(col.ptr_, col.name_, col.subroles_, col.unit_);

    numericals_and_time_stamps.push_back(ts);
  }

  // ---------------------------------------------------------------------------

  if (_time_stamp == "") {
    return DataFrame(categoricals_, discretes_, {indices_.at(ix_join_key)},
                     {join_keys_.at(ix_join_key)}, name_,
                     numericals_and_time_stamps, targets_, text_, {},
                     _row_indices, _word_indices);
  }

  // ---------------------------------------------------------------------------

  const auto ix_time_stamp = find_ix_time_stamp(_time_stamp);

  // ---------------------------------------------------------------------------

  if (_upper_time_stamp == "") {
    return DataFrame(categoricals_, discretes_, {indices_.at(ix_join_key)},
                     {join_keys_.at(ix_join_key)}, name_,
                     numericals_and_time_stamps, targets_, text_,
                     {time_stamps_.at(ix_time_stamp)}, _row_indices,
                     _word_indices);
  }

  // ---------------------------------------------------------------------------

  const auto ix_upper_time_stamp = find_ix_time_stamp(_upper_time_stamp);

  // ---------------------------------------------------------------------------

  return DataFrame(
      categoricals_, discretes_, {indices_.at(ix_join_key)},
      {join_keys_.at(ix_join_key)}, name_, numericals_and_time_stamps, targets_,
      text_,
      {time_stamps_.at(ix_time_stamp), time_stamps_.at(ix_upper_time_stamp)},
      _row_indices, _word_indices);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<const size_t*, const size_t*> DataFrame::find(
    const Int _join_key) const {
  assert_true(indices().size() > 0);

  assert_true(indices_[0]);

  if (std::holds_alternative<InMemoryIndex>(*indices_[0])) {
    const auto& idx = std::get<InMemoryIndex>(*indices_[0]);

    const auto it = idx.find(_join_key);

    if (it == idx.end()) {
      return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
    }

    return std::make_pair(it->second.data(),
                          it->second.data() + it->second.size());
  }

  if (std::holds_alternative<MemoryMappedIndex>(*indices_[0])) {
    const auto& idx = std::get<MemoryMappedIndex>(*indices_[0]);

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

size_t DataFrame::find_ix_join_key(const std::string& _colname) const {
  for (size_t ix_join_key = 0; ix_join_key < join_keys_.size(); ++ix_join_key) {
    if (join_keys_[ix_join_key].name_ == _colname) {
      return ix_join_key;
    }
  }

  const auto get_name = [this](const auto& _col) -> std::string {
    const auto [table, colname] =
        Macros::parse_table_colname(name_, _col.name_);
    return " '" + SQLite3Generator().make_colname(colname) + "',";
  };

  auto names = stl::collect::string(join_keys_ | VIEWS::transform(get_name));

  if (names.size() > 0) {
    names.back() = '.';
  }

  const auto [table, colname] = Macros::parse_table_colname(name_, _colname);

  throw std::runtime_error(
      "Join key named '" + SQLite3Generator().make_colname(colname) +
      "' not found in table '" + table + "'. Found " +
      std::to_string(join_keys_.size()) + " join keys:" + names);

  return 0;
}

// ----------------------------------------------------------------------------

size_t DataFrame::find_ix_time_stamp(const std::string& _colname) const {
  for (size_t ix_time_stamp = 0; ix_time_stamp < time_stamps_.size();
       ++ix_time_stamp) {
    if (time_stamps_[ix_time_stamp].name_ == _colname) {
      return ix_time_stamp;
    }
  }

  const auto get_name = [this](const auto& _col) -> std::string {
    const auto [table, colname] =
        Macros::parse_table_colname(name_, _col.name_);
    return " '" + SQLite3Generator().make_colname(colname) + "',";
  };

  auto names = stl::collect::string(time_stamps_ | VIEWS::transform(get_name));

  if (names.size() > 0) {
    names.back() = '.';
  }

  const auto [table, colname] = Macros::parse_table_colname(name_, _colname);

  throw std::runtime_error(
      "Time stamp named '" + SQLite3Generator().make_colname(colname) +
      "' not found in table '" + table + "'. Found " +
      std::to_string(time_stamps_.size()) + " time stamps:" + names);

  return 0;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
