// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/TextFieldSplitter.hpp"

#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "helpers/Loader.hpp"
#include "helpers/Saver.hpp"
#include "rfl/replace.hpp"

namespace engine {
namespace preprocessors {

containers::DataFrame TextFieldSplitter::add_rowid(
    const containers::DataFrame& _df) const {
  const auto range = fct::iota<Int>(0, _df.nrows());

  const auto ptr =
      std::make_shared<std::vector<Int>>(fct::collect::vector(range));

  const auto rowid = containers::Column<Int>(ptr, helpers::Macros::rowid());

  auto df = _df;

  df.add_int_column(rowid, containers::DataFrame::ROLE_JOIN_KEY);

  return df;
}

// ----------------------------------------------------

containers::DataFrame TextFieldSplitter::remove_text_fields(
    const containers::DataFrame& _df) const {
  const auto get_name = [&_df](const size_t _i) -> std::string {
    return _df.text(_i).name();
  };

  const auto iota = fct::iota<size_t>(0, _df.num_text());

  const auto names = iota | VIEWS::transform(get_name);

  auto df = _df;

  for (const auto name : names) {
    auto col = df.text(name);
    col.set_name(name + helpers::Macros::text_field());
    df.add_string_column(col, containers::DataFrame::ROLE_UNUSED_STRING);
    df.remove_column(name);
  }

  return df;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TextFieldSplitter::fit_transform(const Params& _params) {
  cols_ = fit_df(_params.population_df(), MarkerType::make<"[POPULATION]">());

  for (const auto& df : _params.peripheral_dfs()) {
    const auto new_cols = fit_df(df, MarkerType::make<"[PERIPHERAL]">());

    cols_.insert(cols_.end(), new_cols.begin(), new_cols.end());
  }

  const auto logging_begin =
      (_params.logging_begin() + _params.logging_end()) / 2;

  const auto params =
      rfl::replace(_params, rfl::make_field<"logging_begin_">(logging_begin));

  return transform(params);
}

// ----------------------------------------------------

std::vector<rfl::Ref<helpers::ColumnDescription>> TextFieldSplitter::fit_df(
    const containers::DataFrame& _df, const MarkerType _marker) const {
  const auto to_column_description = [&_df, &_marker](const size_t _i) {
    return rfl::Ref<helpers::ColumnDescription>::make(_marker, _df.name(),
                                                      _df.text(_i).name());
  };

  const auto iota = fct::iota<size_t>(0, _df.num_text());

  return fct::collect::vector(iota | VIEWS::transform(to_column_description));
}

// ----------------------------------------------------

void TextFieldSplitter::load(const std::string& _fname) {
  const auto named_tuple = helpers::Loader::load<ReflectionType>(_fname);
  cols_ = named_tuple.cols();
}

// ----------------------------------------------------

containers::DataFrame TextFieldSplitter::make_new_df(
    const std::shared_ptr<memmap::Pool> _pool, const std::string& _df_name,
    const containers::Column<strings::String>& _col) const {
  const auto [rownums, words] = split_text_fields_on_col(_col);

  auto df = containers::DataFrame(_pool);

  df.set_name(_df_name + helpers::Macros::text_field() + _col.name());

  df.add_int_column(rownums, containers::DataFrame::ROLE_JOIN_KEY);

  df.add_string_column(words, containers::DataFrame::ROLE_TEXT);

  return df;
}

// ----------------------------------------------------

void TextFieldSplitter::save(
    const std::string& _fname,
    const typename helpers::Saver::Format& _format) const {
  helpers::Saver::save(_fname, *this, _format);
}

// ----------------------------------------------------

std::pair<containers::Column<Int>, containers::Column<strings::String>>
TextFieldSplitter::split_text_fields_on_col(
    const containers::Column<strings::String>& _col) const {
  const auto rownums_ptr = std::make_shared<std::vector<Int>>();

  const auto words_ptr = std::make_shared<std::vector<strings::String>>();

  for (size_t i = 0; i < _col.nrows(); ++i) {
    const auto splitted = textmining::Vocabulary::split_text_field(_col[i]);

    for (const auto& word : splitted) {
      rownums_ptr->push_back(i);
      words_ptr->push_back(strings::String(word));
    }
  }

  const auto rownums = containers::Column<Int>(rownums_ptr, "rownum");

  const auto words =
      containers::Column<strings::String>(words_ptr, _col.name());

  return std::make_pair(rownums, words);
}

// ----------------------------------------------------

std::vector<std::string> TextFieldSplitter::to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  assert_true(_sql_dialect_generator);

  const auto split =
      [_sql_dialect_generator](const auto& _desc) -> std::string {
    return _sql_dialect_generator->split_text_fields(_desc.ptr());
  };

  return fct::collect::vector(cols_ | VIEWS::transform(split));
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TextFieldSplitter::transform(const Params& _params) const {
  const auto modify_if_applicable =
      [this](const containers::DataFrame& _df) -> containers::DataFrame {
    return _df.num_text() == 0 ? _df : remove_text_fields(add_rowid(_df));
  };

  const auto population_df = modify_if_applicable(_params.population_df());

  const auto range =
      _params.peripheral_dfs() | VIEWS::transform(modify_if_applicable);

  auto peripheral_dfs = fct::collect::vector(range);

  transform_df(MarkerType::make<"[POPULATION]">(), _params.population_df(),
               &peripheral_dfs);

  for (const auto& df : _params.peripheral_dfs()) {
    transform_df(MarkerType::make<"[PERIPHERAL]">(), df, &peripheral_dfs);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

void TextFieldSplitter::transform_df(
    const MarkerType _marker, const containers::DataFrame& _df,
    std::vector<containers::DataFrame>* _peripheral_dfs) const {
  const auto matching_description = [&_marker,
                                     &_df](const auto& _desc) -> bool {
    return _desc->marker() == _marker && _desc->table() == _df.name();
  };

  const auto get_col =
      [&_df](const auto& _desc) -> containers::Column<strings::String> {
    return _df.text(_desc->name());
  };

  const auto pool = _df.pool()
                        ? std::make_shared<memmap::Pool>(_df.pool()->temp_dir())
                        : std::shared_ptr<memmap::Pool>();

  const auto make_df = [this, pool,
                        &_df](const auto& _col) -> containers::DataFrame {
    const auto df = make_new_df(pool, _df.name(), _col);
    return df;
  };

  auto data_frames = cols_ | VIEWS::filter(matching_description) |
                     VIEWS::transform(get_col) | VIEWS::transform(make_df);

  for (const auto df : data_frames) {
    _peripheral_dfs->push_back(df);
  }
}

}  // namespace preprocessors
}  // namespace engine
