// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/preprocessors/TextFieldSplitter.hpp"

// ----------------------------------------------------

#include "engine/preprocessors/PreprocessorImpl.hpp"

// ----------------------------------------------------

namespace engine {
namespace preprocessors {

containers::DataFrame TextFieldSplitter::add_rowid(
    const containers::DataFrame& _df) const {
  const auto range = fct::iota<Int>(0, _df.nrows());

  const auto ptr =
      std::make_shared<std::vector<Int>>(fct::collect::vector<Int>(range));

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

Poco::JSON::Object::Ptr TextFieldSplitter::fingerprint() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("type_", type());

  obj->set("dependencies_", JSON::vector_to_array_ptr(dependencies_));

  return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TextFieldSplitter::fit_transform(const FitParams& _params) {
  cols_ =
      fit_df(_params.population_df_, helpers::ColumnDescription::POPULATION);

  for (const auto& df : _params.peripheral_dfs_) {
    const auto new_cols = fit_df(df, helpers::ColumnDescription::PERIPHERAL);

    cols_.insert(cols_.end(), new_cols.begin(), new_cols.end());
  }

  const auto params = TransformParams{
      .cmd_ = _params.cmd_,
      .categories_ = _params.categories_,
      .logger_ = _params.logger_,
      .logging_begin_ = (_params.logging_begin_ + _params.logging_end_) / 2,
      .logging_end_ = _params.logging_end_,
      .peripheral_dfs_ = _params.peripheral_dfs_,
      .peripheral_names_ = _params.peripheral_names_,
      .placeholder_ = _params.placeholder_,
      .population_df_ = _params.population_df_};

  return transform(params);
}

// ----------------------------------------------------

std::vector<std::shared_ptr<helpers::ColumnDescription>>
TextFieldSplitter::fit_df(const containers::DataFrame& _df,
                          const std::string& _marker) const {
  const auto to_column_description = [&_df, &_marker](const size_t _i) {
    return std::make_shared<helpers::ColumnDescription>(_marker, _df.name(),
                                                        _df.text(_i).name());
  };

  const auto iota = fct::iota<size_t>(0, _df.num_text());

  const auto range = iota | VIEWS::transform(to_column_description);

  return fct::collect::vector<std::shared_ptr<helpers::ColumnDescription>>(
      range);
}

// ----------------------------------------------------

TextFieldSplitter TextFieldSplitter::from_json_obj(
    const Poco::JSON::Object& _obj) const {
  TextFieldSplitter that;

  if (_obj.has("cols_")) {
    that.cols_ = PreprocessorImpl::from_array(
        jsonutils::JSON::get_object_array(_obj, "cols_"));
  }

  return that;
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

Poco::JSON::Object::Ptr TextFieldSplitter::to_json_obj() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("type_", type());

  obj->set("cols_", PreprocessorImpl::to_array(cols_));

  return obj;
}

// ----------------------------------------------------

std::vector<std::string> TextFieldSplitter::to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  assert_true(_sql_dialect_generator);

  const auto split =
      [_sql_dialect_generator](
          const std::shared_ptr<helpers::ColumnDescription>& _desc)
      -> std::string {
    return _sql_dialect_generator->split_text_fields(_desc);
  };

  return fct::collect::vector<std::string>(cols_ | VIEWS::transform(split));
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TextFieldSplitter::transform(const TransformParams& _params) const {
  const auto modify_if_applicable =
      [this](const containers::DataFrame& _df) -> containers::DataFrame {
    return _df.num_text() == 0 ? _df : remove_text_fields(add_rowid(_df));
  };

  const auto population_df = modify_if_applicable(_params.population_df_);

  const auto range =
      _params.peripheral_dfs_ | VIEWS::transform(modify_if_applicable);

  auto peripheral_dfs = fct::collect::vector<containers::DataFrame>(range);

  transform_df(helpers::ColumnDescription::POPULATION, _params.population_df_,
               &peripheral_dfs);

  for (const auto& df : _params.peripheral_dfs_) {
    transform_df(helpers::ColumnDescription::PERIPHERAL, df, &peripheral_dfs);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

void TextFieldSplitter::transform_df(
    const std::string& _marker, const containers::DataFrame& _df,
    std::vector<containers::DataFrame>* _peripheral_dfs) const {
  // ----------------------------------------------------

  const auto matching_description =
      [&_marker,
       &_df](const std::shared_ptr<helpers::ColumnDescription>& _desc) -> bool {
    assert_true(_desc);
    return _desc->marker_ == _marker && _desc->table_ == _df.name();
  };

  // ----------------------------------------------------

  const auto get_col =
      [&_df](const std::shared_ptr<helpers::ColumnDescription>& _desc)
      -> containers::Column<strings::String> {
    assert_true(_desc);
    return _df.text(_desc->name_);
  };

  // ----------------------------------------------------

  const auto pool = _df.pool()
                        ? std::make_shared<memmap::Pool>(_df.pool()->temp_dir())
                        : std::shared_ptr<memmap::Pool>();

  const auto make_df = [this, pool,
                        &_df](const containers::Column<strings::String>& _col)
      -> containers::DataFrame {
    const auto df = make_new_df(pool, _df.name(), _col);
    return df;
  };

  // ----------------------------------------------------

  auto data_frames = cols_ | VIEWS::filter(matching_description) |
                     VIEWS::transform(get_col) | VIEWS::transform(make_df);

  for (const auto df : data_frames) {
    _peripheral_dfs->push_back(df);
  }

  // ----------------------------------------------------
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
