// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/Substring.hpp"

#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "helpers/Loader.hpp"
#include "helpers/Saver.hpp"

namespace engine {
namespace preprocessors {

std::optional<containers::Column<Int>> Substring::extract_substring(
    const containers::Column<strings::String>& _col,
    containers::Encoding* _categories) const {
  auto str_col = extract_substring_string(_col);

  auto int_col = containers::Column<Int>(_col.pool(), str_col.nrows());

  for (size_t i = 0; i < str_col.nrows(); ++i) {
    int_col[i] = (*_categories)[str_col[i]];
  }

  int_col.set_name(make_name(_col.name()));

  int_col.set_unit(make_unit(_col.unit()));

  if (PreprocessorImpl::has_warnings(int_col)) {
    return std::nullopt;
  }

  return int_col;
}

// ----------------------------------------------------

containers::Column<Int> Substring::extract_substring(
    const containers::Encoding& _categories,
    const containers::Column<strings::String>& _col) const {
  auto str_col = extract_substring_string(_col);

  auto int_col = containers::Column<Int>(_col.pool(), str_col.nrows());

  for (size_t i = 0; i < str_col.nrows(); ++i) {
    int_col[i] = _categories[str_col[i]];
  }

  int_col.set_name(make_name(_col.name()));

  int_col.set_unit(make_unit(_col.unit()));

  return int_col;
}

// ----------------------------------------------------

containers::Column<strings::String> Substring::extract_substring_string(
    const containers::Column<strings::String>& _col) const {
  auto result = containers::Column<strings::String>(_col.pool());

  for (size_t i = 0; i < _col.nrows(); ++i) {
    const auto str = _col[i].str();

    const auto substr = str.substr(begin_, length_);

    result.push_back(strings::String(substr));
  }

  return result;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Substring::fit_transform(const Params& _params) {
  const auto population_df = fit_transform_df(
      _params.population_df(), MarkerType::make<"[POPULATION]">(), 0,
      _params.categories().get());

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs().size(); ++i) {
    const auto& df = _params.peripheral_dfs().at(i);

    const auto new_df = fit_transform_df(df, MarkerType::make<"[PERIPHERAL]">(),
                                         i, _params.categories().get());

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame Substring::fit_transform_df(
    const containers::DataFrame& _df, const MarkerType _marker,
    const size_t _table, containers::Encoding* _categories) {
  auto df = _df;

  for (size_t i = 0; i < _df.num_categoricals(); ++i) {
    const auto& original_col = _df.categorical(i);

    extract_and_add(_marker, _table, original_col, _categories, &df);
  }

  for (size_t i = 0; i < _df.num_text(); ++i) {
    const auto& original_col = _df.text(i);

    extract_and_add(_marker, _table, original_col, _categories, &df);
  }

  return df;
}

// -----------------------------------------------------------------------------

void Substring::load(const std::string& _fname) {
  const auto named_tuple = helpers::Loader::load<ReflectionType>(_fname);
  cols_ = named_tuple.cols();
}

// ----------------------------------------------------

containers::Column<strings::String> Substring::make_str_col(
    const containers::Encoding& _categories,
    const containers::Column<Int>& _col) const {
  auto result = containers::Column<strings::String>(_col.pool());

  for (size_t i = 0; i < _col.nrows(); ++i) {
    result.push_back(_categories[_col[i]]);
  }

  result.set_name(_col.name());
  result.set_unit(_col.unit());

  return result;
}

// ----------------------------------------------------

void Substring::save(const std::string& _fname,
                     const typename helpers::Saver::Format& _format) const {
  helpers::Saver::save(_fname, *this, _format);
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Substring::transform(const Params& _params) const {
  const auto population_df =
      transform_df(*_params.categories(), _params.population_df(),
                   MarkerType::make<"[POPULATION]">(), 0);

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs().size(); ++i) {
    const auto& df = _params.peripheral_dfs().at(i);

    const auto new_df = transform_df(*_params.categories(), df,
                                     MarkerType::make<"[PERIPHERAL]">(), i);

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame Substring::transform_df(
    const containers::Encoding& _categories, const containers::DataFrame& _df,
    const MarkerType _marker, const size_t _table) const {
  auto df = _df;

  auto names = PreprocessorImpl::retrieve_names(_marker, _table, cols_);

  for (const auto& name : names) {
    if (_df.has_categorical(name)) {
      const auto col = extract_substring(_categories, _df.categorical(name));

      df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
    } else if (_df.has_text(name)) {
      const auto col = extract_substring(_categories, _df.text(name));

      df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
    } else {
      throw std::runtime_error("'" + _df.name() +
                               "' has no categorical or text column named '" +
                               name + "'!");
    }
  }

  // ----------------------------------------------------

  return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
