#include "engine/preprocessors/Mapping.hpp"

// ----------------------------------------------------

#include "engine/preprocessors/PreprocessorImpl.hpp"

// ----------------------------------------------------

namespace engine {
namespace preprocessors {

std::tuple<helpers::DataFrame, std::optional<helpers::TableHolder>,
           std::shared_ptr<const helpers::VocabularyContainer>>
Mapping::build_prerequisites(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const bool _targets) const {
  assert_true(population_schema_);

  assert_true(peripheral_schema_);

  const auto infer_needs_targets = [this, _targets, &_placeholder,
                                    &_peripheral_names]() -> std::vector<bool> {
    if (_targets) {
      return std::vector<bool>(peripheral_schema_->size(), true);
    }

    auto needs_targets = _placeholder.infer_needs_targets(_peripheral_names);

    if (peripheral_schema_->size() > needs_targets.size()) {
      needs_targets.insert(needs_targets.end(),
                           peripheral_schema_->size() - needs_targets.size(),
                           false);
    }

    return needs_targets;
  };

  const auto needs_targets = infer_needs_targets();

  const auto to_immutable = [this, &_peripheral_dfs, &needs_targets](
                                const size_t _i) -> helpers::DataFrame {
    return _peripheral_dfs.at(_i).to_immutable<helpers::DataFrame>(
        peripheral_schema_->at(_i), needs_targets.at(_i));
  };

  if (peripheral_schema_->size() != _peripheral_dfs.size()) {
    throw std::runtime_error("Mapping: Expected " +
                             std::to_string(peripheral_schema_->size()) +
                             " peripheral tables, got " +
                             std::to_string(_peripheral_dfs.size()) + ".");
  }

  const auto population =
      _population_df.to_immutable<helpers::DataFrame>(*population_schema_);

  const auto iota = fct::iota<size_t>(0, _peripheral_dfs.size());

  const auto peripheral = fct::collect::vector<helpers::DataFrame>(
      iota | VIEWS::transform(to_immutable));

  const auto [vocabulary, word_index_container] =
      handle_text_fields(population, peripheral);

  const auto rownums = std::make_shared<std::vector<size_t>>(
      fct::collect::vector<size_t>(fct::iota<size_t>(0, population.nrows())));

  const auto population_view = helpers::DataFrameView(population, rownums);

  const auto make_staging_table_colname =
      [](const std::string& _colname) -> std::string {
    return transpilation::SQLite3Generator().make_staging_table_colname(
        _colname);
  };

  const auto table_holder_params = helpers::TableHolderParams{
      .feature_container_ = std::nullopt,
      .make_staging_table_colname_ = make_staging_table_colname,
      .peripheral_ = peripheral,
      .peripheral_names_ = _peripheral_names,
      .placeholder_ = _placeholder,
      .population_ = population_view,
      .row_index_container_ = std::nullopt,
      .word_index_container_ = word_index_container};

  const auto table_holder =
      peripheral.size() > 0
          ? std::make_optional<helpers::TableHolder>(table_holder_params)
          : std::optional<helpers::TableHolder>();

  const auto data_frame_params = helpers::DataFrameParams{
      .categoricals_ = population.categoricals_,
      .discretes_ = population.discretes_,
      .indices_ = population.indices_,
      .join_keys_ = population.join_keys_,
      .name_ = population.name_,
      .numericals_ = population.numericals_,
      .targets_ = population.targets_,
      .text_ = population.text_,
      .time_stamps_ = population.time_stamps_,
      .ts_index_ = population.ts_index_,
      .word_indices_ = word_index_container.population()};

  const auto population_with_word_indices =
      helpers::DataFrame(data_frame_params);

  return std::make_tuple(population_with_word_indices, table_holder,
                         vocabulary);
}

// ----------------------------------------------------

std::pair<Int, std::vector<Float>> Mapping::calc_agg_targets(
    const helpers::DataFrame& _population,
    const std::pair<Int, std::vector<size_t>>& _input) const {
  const auto& rownums = _input.second;

  const auto calc_aggs =
      [this, &rownums](
          const helpers::Column<Float>& _target_col) -> std::vector<Float> {
    const auto get_value = [&_target_col](const size_t _i) -> Float {
      return _target_col[_i];
    };

    const auto value_range = rownums | VIEWS::transform(get_value);

    const auto agg = [this,
                      value_range](const MappingAggregation& _agg) -> Float {
      return aggregate(value_range.begin(), value_range.end(), _agg);
    };

    const auto aggregated_range = aggregation_enums_ | VIEWS::transform(agg);

    return fct::collect::vector<Float>(aggregated_range);
  };

  const auto range = _population.targets_ | VIEWS::transform(calc_aggs);

  const auto second = fct::join::vector<Float>(range);

  return std::make_pair(_input.first, second);
}

// ----------------------------------------------------

std::vector<std::string> Mapping::categorical_columns_to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  assert_true(categorical_names_);

  assert_true(_sql_dialect_generator);

  const auto mapping_to_sql = [this, &_categories, &_sql_dialect_generator](
                                  const size_t _i,
                                  const size_t _weight_num) -> std::string {
    const auto name = _sql_dialect_generator->make_staging_table_colname(
        make_staging_table_colname(categorical_names_->at(_i), _weight_num));

    return categorical_or_text_column_to_sql(
        _categories, _sql_dialect_generator,
        transpilation::SQLGenerator::to_upper(name), categorical_.at(_i),
        _weight_num, false);
  };

  return columns_to_sql(mapping_to_sql, categorical_, categorical_names_);
}

// ----------------------------------------------------

std::string Mapping::categorical_or_text_column_to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _name, const PtrType& _ptr, const size_t _weight_num,
    const bool _is_text) const {
  assert_true(_ptr);

  assert_true(_sql_dialect_generator);

  const auto pairs = make_pairs(*_ptr, _weight_num);

  std::stringstream sql;

  sql << _sql_dialect_generator->make_mapping_table_header(_name, false);

  constexpr size_t batch_size = 500;

  for (size_t batch_begin = 0; batch_begin < pairs.size();
       batch_begin += batch_size) {
    const auto batch_end = std::min(pairs.size(), batch_begin + batch_size);

    sql << _sql_dialect_generator->make_mapping_table_insert_into(_name);

    for (size_t i = batch_begin; i < batch_end; ++i) {
      const std::string begin = (i == batch_begin) ? "" : "      ";

      const auto& p = pairs.at(i);

      assert_true(p.first >= 0);

      assert_true(static_cast<size_t>(p.first) < _categories.size());

      const std::string end = (i == batch_end - 1) ? ";\n\n" : ",\n";

      sql << begin << "('" << _categories.at(p.first).str() << "', "
          << io::Parser::to_precise_string(p.second) << ")" << end;
    }
  }

  assert_true(_sql_dialect_generator);

  sql << _sql_dialect_generator->join_mapping(table_name_, _name, _is_text);

  return sql.str();
}

// ----------------------------------------------------

std::string Mapping::discrete_column_to_sql(
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _name, const PtrType& _ptr,
    const size_t _weight_num) const {
  assert_true(_ptr);

  assert_true(_sql_dialect_generator);

  const auto pairs = make_pairs(*_ptr, _weight_num);

  std::stringstream sql;

  sql << _sql_dialect_generator->make_mapping_table_header(_name, true);

  constexpr size_t batch_size = 500;

  for (size_t batch_begin = 0; batch_begin < pairs.size();
       batch_begin += batch_size) {
    const auto batch_end = std::min(pairs.size(), batch_begin + batch_size);

    sql << _sql_dialect_generator->make_mapping_table_insert_into(_name);

    for (size_t i = batch_begin; i < batch_end; ++i) {
      const std::string begin = (i == batch_begin) ? "" : "      ";

      const auto& p = pairs.at(i);

      const std::string end = (i == batch_end - 1) ? ";\n\n" : ",\n";

      sql << begin << "(" << std::to_string(p.first) << ", "
          << io::Parser::to_precise_string(p.second) << ")" << end;
    }
  }

  assert_true(_sql_dialect_generator);

  sql << _sql_dialect_generator->join_mapping(table_name_, _name, false);

  return sql.str();
}

// ----------------------------------------------------

std::vector<std::string> Mapping::discrete_columns_to_sql(
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  assert_true(discrete_names_);

  assert_true(_sql_dialect_generator);

  const auto mapping_to_sql = [this, &_sql_dialect_generator](
                                  const size_t _i,
                                  const size_t _weight_num) -> std::string {
    const auto name = _sql_dialect_generator->make_staging_table_colname(
        make_staging_table_colname(discrete_names_->at(_i), _weight_num));

    return discrete_column_to_sql(_sql_dialect_generator,
                                  transpilation::SQLGenerator::to_upper(name),
                                  discrete_.at(_i), _weight_num);
  };

  return columns_to_sql(mapping_to_sql, discrete_, discrete_names_);
}

// ----------------------------------------------------------------------------

typename Mapping::MappingForDf Mapping::extract_mapping(
    const Poco::JSON::Object& _obj, const std::string& _key) const {
  const auto arr = *JSON::get_array(_obj, _key);

  const auto obj_to_map = [&arr](const size_t _i) {
    auto ptr = arr.getObject(_i);

    throw_unless(ptr, "Expected an object inside the mapping.");

    auto m = std::make_shared<std::map<Int, std::vector<Float>>>();

    for (const auto& [key, _] : *ptr) {
      const auto arr = ptr->getArray(key);

      throw_unless(arr, "Expected an array: key: " + key +
                            ", _obj: " + jsonutils::JSON::stringify(*ptr));

      (*m)[static_cast<Int>(std::atoi(key.c_str()))] =
          jsonutils::JSON::array_to_vector<Float>(arr);
    }

    return m;
  };

  const auto iota = fct::iota<size_t>(0, arr.size());

  const auto range = iota | VIEWS::transform(obj_to_map);

  return fct::collect::vector<MappingForDf::value_type>(range);
}

// ----------------------------------------------------

typename Mapping::TextMapping Mapping::extract_text_mapping(
    const Poco::JSON::Object& _obj, const std::string& _key) const {
  const auto obj_to_map = [](const Poco::JSON::Object& _obj) {
    auto m = std::make_shared<std::map<std::string, std::vector<Float>>>();

    for (const auto& [key, _] : _obj) {
      const auto arr = _obj.getArray(key);

      assert_msg(arr,
                 "key: " + key + ", _obj: " + jsonutils::JSON::stringify(_obj));

      (*m)[key] = jsonutils::JSON::array_to_vector<Float>(arr);
    }

    return m;
  };

  const auto arr = *JSON::get_array(_obj, _key);

  const auto get_obj = [&arr](const size_t _i) -> Poco::JSON::Object {
    auto ptr = arr.getObject(_i);
    throw_unless(ptr, "Expected an object inside the mapping.");
    return *ptr;
  };

  const auto iota = fct::iota<size_t>(0, arr.size());

  const auto range =
      iota | VIEWS::transform(get_obj) | VIEWS::transform(obj_to_map);

  return fct::collect::vector<TextMapping::value_type>(range);
}

// ----------------------------------------------------

std::vector<size_t> Mapping::find_output_ix(
    const std::vector<size_t>& _input_ix,
    const helpers::DataFrame& _output_table,
    const helpers::DataFrame& _input_table) const {
  const auto time_stamp_in_range = [](const Float _time_stamp_input,
                                      const Float _upper_time_stamp,
                                      const Float _time_stamp_output) -> bool {
    return ((_time_stamp_input <= _time_stamp_output) &&
            (std::isnan(_upper_time_stamp) ||
             _time_stamp_output < _upper_time_stamp));
  };

  std::vector<size_t> result;

  for (const auto ix : _input_ix) {
    if (!_output_table.has(_input_table.join_key(ix))) {
      continue;
    }

    const auto [begin, end] = _output_table.find(_input_table.join_key(ix));

    const auto time_stamp_input = _input_table.time_stamp(ix);

    const auto upper_time_stamp = _input_table.upper_time_stamp(ix);

    for (auto it = begin; it != end; ++it) {
      const auto ix_out = *it;

      assert_true(ix_out >= 0);
      assert_true(ix_out < _output_table.nrows());

      const bool use_this = time_stamp_in_range(
          time_stamp_input, upper_time_stamp, _output_table.time_stamp(ix_out));

      if (use_this) {
        result.push_back(static_cast<size_t>(ix_out));
      }
    }
  }

  return result;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Mapping::fingerprint() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("type_", type());

  obj->set("dependencies_", JSON::vector_to_array_ptr(dependencies_));

  obj->set("aggregation_", JSON::vector_to_array_ptr(aggregation_));

  obj->set("min_freq_", min_freq_);

  return obj;
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_categoricals(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _logger) const {
  const auto include = [this](const helpers::Column<Int>& _col) -> bool {
    return parse_subroles(_col);
  };

  const auto col_to_mapping = [this, &_population, &_main_tables,
                               &_peripheral_tables,
                               _logger](const helpers::Column<Int>& _col) {
    const auto rownum_map = make_rownum_map_categorical(_col);
    return make_mapping(rownum_map, _population, _main_tables,
                        _peripheral_tables, _logger);
  };

  const auto get_colname = [](const helpers::Column<Int>& _col) -> std::string {
    return _col.name_;
  };

  const auto& data_frame =
      _peripheral_tables.size() > 0 ? _peripheral_tables.back() : _population;

  const auto range1 = data_frame.categoricals_ | VIEWS::filter(include) |
                      VIEWS::transform(col_to_mapping);

  const auto range2 = data_frame.categoricals_ | VIEWS::filter(include) |
                      VIEWS::transform(get_colname);

  const auto mappings = fct::collect::vector<MappingForDf::value_type>(range1);

  const auto colnames = std::make_shared<const std::vector<std::string>>(
      fct::collect::vector<std::string>(range2));

  return std::make_pair(mappings, colnames);
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_discretes(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _logger) const {
  const auto include = [this](const helpers::Column<Float>& _col) -> bool {
    return parse_subroles(_col);
  };

  const auto col_to_mapping = [this, &_population, &_main_tables,
                               &_peripheral_tables,
                               _logger](const helpers::Column<Float>& _col) {
    const auto rownum_map = make_rownum_map_discrete(_col);
    return make_mapping(rownum_map, _population, _main_tables,
                        _peripheral_tables, _logger);
  };

  const auto get_colname =
      [](const helpers::Column<Float>& _col) -> std::string {
    return _col.name_;
  };

  const auto& data_frame =
      _peripheral_tables.size() > 0 ? _peripheral_tables.back() : _population;

  const auto range1 = data_frame.discretes_ | VIEWS::filter(include) |
                      VIEWS::transform(col_to_mapping);

  const auto range2 = data_frame.discretes_ | VIEWS::filter(include) |
                      VIEWS::transform(get_colname);

  const auto mappings = fct::collect::vector<MappingForDf::value_type>(range1);

  const auto colnames = std::make_shared<const std::vector<std::string>>(
      fct::collect::vector<std::string>(range2));

  return std::make_pair(mappings, colnames);
}

// ----------------------------------------------------

Mapping Mapping::fit_on_table_holder(
    const helpers::DataFrame& _population,
    const helpers::TableHolder& _table_holder,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables, const size_t _ix,
    logging::ProgressLogger* _logger) const {
  const auto append =
      [](const std::vector<helpers::DataFrame>& _vec,
         const helpers::DataFrame& _df) -> std::vector<helpers::DataFrame> {
    auto vec = _vec;
    vec.push_back(_df);
    return vec;
  };

  assert_true(_table_holder.main_tables().size() ==
              _table_holder.peripheral_tables().size());

  assert_true(_table_holder.main_tables().size() ==
              _table_holder.subtables().size());

  assert_true(_ix < _table_holder.main_tables().size());

  const auto main_tables =
      append(_main_tables, _table_holder.main_tables().at(_ix).df());

  const auto peripheral_tables =
      append(_peripheral_tables, _table_holder.peripheral_tables().at(_ix));

  auto mapping = *this;

  mapping.prefix_ += std::to_string(_ix + 1) + "_";

  mapping.table_name_ = _table_holder.peripheral_tables().at(_ix).name();

  mapping.submappings_ =
      mapping.fit_submappings(_population, _table_holder.subtables().at(_ix),
                              main_tables, peripheral_tables, _logger);

  std::tie(mapping.categorical_, mapping.categorical_names_) =
      fit_on_categoricals(_population, main_tables, peripheral_tables, _logger);

  std::tie(mapping.discrete_, mapping.discrete_names_) =
      fit_on_discretes(_population, main_tables, peripheral_tables, _logger);

  std::tie(mapping.text_, mapping.text_names_) =
      fit_on_text(_population, main_tables, peripheral_tables, _logger);

  return mapping;
}

// ----------------------------------------------------------------------------

std::pair<typename Mapping::MappingForDf, typename Mapping::Colnames>
Mapping::fit_on_text(const helpers::DataFrame& _population,
                     const std::vector<helpers::DataFrame>& _main_tables,
                     const std::vector<helpers::DataFrame>& _peripheral_tables,
                     logging::ProgressLogger* _logger) const {
  const auto word_index_to_mapping =
      [this, &_population, &_main_tables, &_peripheral_tables, _logger](
          const std::shared_ptr<const textmining::WordIndex>& _word_index) {
        assert_true(_word_index);
        const auto rownum_map = make_rownum_map_text(*_word_index);
        return make_mapping(rownum_map, _population, _main_tables,
                            _peripheral_tables, _logger);
      };

  const auto get_colname =
      [](const helpers::Column<strings::String>& _col) -> std::string {
    return _col.name_;
  };

  const auto include =
      [this](const helpers::Column<strings::String>& _col) -> bool {
    return parse_subroles(_col);
  };

  const auto& data_frame =
      _peripheral_tables.size() > 0 ? _peripheral_tables.back() : _population;

  const auto include_index = [&data_frame, include](const size_t _i) -> bool {
    return include(data_frame.text_.at(_i));
  };

  const auto get_word_index =
      [&data_frame](
          const size_t _i) -> std::shared_ptr<const textmining::WordIndex> {
    return data_frame.word_indices_.at(_i);
  };

  assert_msg(data_frame.word_indices_.size() == data_frame.text_.size(),
             "data_frame.word_indices_.size(): " +
                 std::to_string(data_frame.word_indices_.size()) +
                 ", data_frame.text_.size(): " +
                 std::to_string(data_frame.text_.size()));

  const auto iota = fct::iota<size_t>(0, data_frame.text_.size());

  const auto range1 = iota | VIEWS::filter(include_index) |
                      VIEWS::transform(get_word_index) |
                      VIEWS::transform(word_index_to_mapping);

  const auto range2 =
      data_frame.text_ | VIEWS::filter(include) | VIEWS::transform(get_colname);

  const auto mappings = fct::collect::vector<MappingForDf::value_type>(range1);

  const auto colnames = std::make_shared<const std::vector<std::string>>(
      fct::collect::vector<std::string>(range2));

  return std::make_pair(mappings, colnames);
}

// ----------------------------------------------------

std::vector<Mapping> Mapping::fit_submappings(
    const helpers::DataFrame& _population,
    const std::optional<helpers::TableHolder>& _table_holder,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _logger) const {
  if (!_table_holder) {
    return {};
  }

  const auto fit = [this, &_population, &_table_holder, &_main_tables,
                    &_peripheral_tables, _logger](const size_t _ix) -> Mapping {
    return fit_on_table_holder(_population, _table_holder.value(), _main_tables,
                               _peripheral_tables, _ix, _logger);
  };

  const auto iota = fct::iota<size_t>(0, _table_holder->main_tables().size());

  return fct::collect::vector<Mapping>(iota | VIEWS::transform(fit));
}

// ----------------------------------------------------

std::pair<std::shared_ptr<const containers::Schema>,
          std::shared_ptr<const std::vector<containers::Schema>>>
Mapping::extract_schemata(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs) const {
  const auto to_schema = [](const containers::DataFrame& _df) {
    return _df.to_schema(true);
  };

  const auto population_schema = std::make_shared<const containers::Schema>(
      _population_df.to_schema(true));

  const auto peripheral_schema =
      std::make_shared<const std::vector<containers::Schema>>(
          fct::collect::vector<containers::Schema>(
              _peripheral_dfs | VIEWS::transform(to_schema)));

  return std::make_pair(population_schema, peripheral_schema);
}

// ----------------------------------------------------

size_t Mapping::infer_num_cols(
    const helpers::TableHolder& _table_holder) const {
  if (_table_holder.main_tables().size() == 0) {
    return 0;
  }

  const auto infer_peripheral =
      [this](const helpers::DataFrame& _df) -> size_t {
    return infer_num_cols(_df);
  };

  const auto infer_subtable =
      [this](const std::optional<helpers::TableHolder>& _subtable) -> size_t {
    if (!_subtable) {
      return 0;
    }
    return infer_num_cols(*_subtable);
  };

  auto range_peripherals =
      _table_holder.peripheral_tables() | VIEWS::transform(infer_peripheral);

  auto range_subtables =
      _table_holder.subtables() | VIEWS::transform(infer_subtable);

  const auto num_cols_population =
      infer_num_cols(_table_holder.main_tables().at(0).df());

  const auto num_cols_peripherals =
      std::accumulate(range_peripherals.begin(), range_peripherals.end(), 0);

  const auto num_cols_subtables =
      std::accumulate(range_subtables.begin(), range_subtables.end(), 0);

  return num_cols_population + num_cols_peripherals + num_cols_subtables;
}

// ----------------------------------------------------

size_t Mapping::infer_num_cols(const helpers::DataFrame& _df) const {
  const auto include = [this](const auto& _col) -> bool {
    return parse_subroles(_col);
  };

  const auto get_length = [](auto _range) -> size_t {
    return static_cast<size_t>(std::distance(_range.begin(), _range.end()));
  };

  const auto range1 = _df.categoricals_ | VIEWS::filter(include);

  const auto range2 = _df.discretes_ | VIEWS::filter(include);

  const auto range3 = _df.text_ | VIEWS::filter(include);

  return get_length(range1) + get_length(range2) + get_length(range3);
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Mapping::fit_transform(const FitParams& _params) {
  assert_true(_params.categories_);

  std::tie(population_schema_, peripheral_schema_) =
      extract_schemata(_params.population_df_, _params.peripheral_dfs_);

  const auto [population, table_holder, vocabulary] = build_prerequisites(
      _params.population_df_, _params.peripheral_dfs_, _params.placeholder_,
      _params.peripheral_names_, true);

  const auto num_cols =
      table_holder ? infer_num_cols(*table_holder) : infer_num_cols(population);

  auto logger =
      logging::ProgressLogger("", _params.logger_, num_cols * 2,
                              _params.logging_begin_, _params.logging_end_);

  vocabulary_ = vocabulary;

  submappings_ = fit_submappings(population, table_holder, {}, {}, &logger);

  table_name_ = _params.population_df_.name();

  std::tie(categorical_, categorical_names_) =
      fit_on_categoricals(population, {}, {}, &logger);

  std::tie(discrete_, discrete_names_) =
      fit_on_discretes(population, {}, {}, &logger);

  std::tie(text_, text_names_) = fit_on_text(population, {}, {}, &logger);

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

Mapping Mapping::from_json_obj(const Poco::JSON::Object& _obj) const {
  const auto parse = [this](const std::string& _str) -> MappingAggregation {
    return parse_aggregation(_str);
  };

  const auto extract_mpg = [this](const Poco::JSON::Object& _obj,
                                  const std::string& _name) {
    return extract_mapping(_obj, _name);
  };

  const auto extract_colnames = [](const Poco::JSON::Object& _obj,
                                   const std::string& _key) {
    auto arr = _obj.getArray(_key);
    throw_unless(arr, "Expected array called '" + _key + "'");
    return std::make_shared<const std::vector<std::string>>(
        JSON::array_to_vector<std::string>(arr));
  };

  const auto extract_peripheral_schema = [&_obj]() {
    const auto arr = JSON::get_array(_obj, "peripheral_schema_");

    const auto to_schema = [&arr](const size_t _i) {
      auto ptr = arr->getObject(_i);
      throw_unless(ptr, "Mapping: Expected an object");
      return containers::Schema::from_json(*ptr);
    };

    const auto iota = fct::iota<size_t>(0, arr->size());

    return std::make_shared<std::vector<containers::Schema>>(
        fct::collect::vector<containers::Schema>(iota |
                                                 VIEWS::transform(to_schema)));
  };

  auto that = *this;

  that.aggregation_ =
      JSON::array_to_vector<std::string>(JSON::get_array(_obj, "aggregation_"));

  that.aggregation_enums_ = fct::collect::vector<MappingAggregation>(
      that.aggregation_ | VIEWS::transform(parse));

  that.dependencies_ = dependencies_;

  that.min_freq_ = JSON::get_value<size_t>(_obj, "min_freq_");

  that.multithreading_ = JSON::get_value<bool>(_obj, "multithreading_");

  if (!_obj.has("categorical_")) {
    return that;
  }

  that.categorical_ = extract_mpg(_obj, "categorical_");

  that.categorical_names_ = extract_colnames(_obj, "categorical_names_");

  that.discrete_ = extract_mpg(_obj, "discrete_");

  that.discrete_names_ = extract_colnames(_obj, "discrete_names_");

  that.prefix_ = JSON::get_value<std::string>(_obj, "prefix_");

  that.table_name_ = JSON::get_value<std::string>(_obj, "table_name_");

  that.text_ = extract_mpg(_obj, "text_");

  that.text_names_ = extract_colnames(_obj, "text_names_");

  if (that.prefix_ == "") {
    that.peripheral_schema_ = extract_peripheral_schema();

    that.population_schema_ = std::make_shared<const containers::Schema>(
        containers::Schema::from_json(
            *JSON::get_object(_obj, "population_schema_")));

    that.vocabulary_ = std::make_shared<const helpers::VocabularyContainer>(
        *JSON::get_object(_obj, "vocabulary_"));
  }

  assert_true(that.peripheral_schema_);

  assert_true(that.population_schema_);

  assert_true(that.vocabulary_);

  const auto extract_submappings = [&that, &_obj]() {
    const auto arr = JSON::get_array(_obj, "submappings_");

    const auto to_mapping = [that, &arr](const size_t _i) {
      auto ptr = arr->getObject(_i);
      throw_unless(ptr, "Mapping: Expected an object");
      auto mapping = that;
      return mapping.from_json_obj(*ptr);
    };

    const auto iota = fct::iota<size_t>(0, arr->size());

    return fct::collect::vector<Mapping>(iota | VIEWS::transform(to_mapping));
  };

  that.submappings_ = extract_submappings();

  return that;
}

// ----------------------------------------------------

std::pair<std::shared_ptr<const helpers::VocabularyContainer>,
          helpers::WordIndexContainer>
Mapping::handle_text_fields(
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _peripheral) const {
  const auto vocabulary =
      vocabulary_ ? vocabulary_
                  : std::make_shared<const helpers::VocabularyContainer>(
                        1, 0, _population, _peripheral);

  const auto word_indices =
      helpers::WordIndexContainer(_population, _peripheral, *vocabulary);

  return std::make_pair(vocabulary, word_indices);
}

// ----------------------------------------------------

std::string Mapping::make_staging_table_colname(
    const std::string& _name, const size_t _weight_num) const {
  const auto agg_num = _weight_num % aggregation_.size();

  const auto target_num = _weight_num / aggregation_.size();

  return _name + "__mapping_" + prefix_ + "target_" +
         std::to_string(target_num + 1) + "_" +
         transpilation::SQLGenerator::to_lower(aggregation_.at(agg_num));
}

// ----------------------------------------------------

std::shared_ptr<const std::map<Int, std::vector<Float>>> Mapping::make_mapping(
    const std::map<Int, std::vector<size_t>>& _rownum_map,
    const helpers::DataFrame& _population,
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _logger) const {
  const auto match_rows = [this, &_main_tables, &_peripheral_tables](
                              const RownumPair& _input) -> RownumPair {
    return match_rownums(_main_tables, _peripheral_tables, _input);
  };

  const auto greater_than_min_freq = [this](const RownumPair& _input) -> bool {
    const auto& vec = _input.second;
    const auto unique = std::set<size_t>(vec.begin(), vec.end());
    return unique.size() >= min_freq_;
  };

  const auto calc_agg =
      [this, &_population](
          const RownumPair& _pair) -> std::pair<Int, std::vector<Float>> {
    return calc_agg_targets(_population, _pair);
  };

  auto range = _rownum_map | VIEWS::transform(match_rows) |
               VIEWS::filter(greater_than_min_freq) |
               VIEWS::transform(calc_agg);

#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
#else
  if (multithreading_) {
    return std::make_shared<const std::map<Int, std::vector<Float>>>(
        fct::collect_parallel::map<Int, std::vector<Float>>(range));
  }
#endif

  const auto mapping =
      std::make_shared<const std::map<Int, std::vector<Float>>>(range.begin(),
                                                                range.end());

  _logger->increment();

  return mapping;
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::make_mapping_columns_int(
    const std::pair<containers::Column<Int>, MappingForDf::value_type>& _p,
    logging::ProgressLogger* _logger) const {
  // ----------------------------------------------------

  const auto& col = _p.first;
  const auto& mapping = _p.second;

  assert_true(mapping);

  // ----------------------------------------------------

  const auto map_value = [&mapping](const size_t _weight_num,
                                    const Int& _val) -> Float {
    const auto it = mapping->find(_val);

    if (it == mapping->end()) {
      return 0.0;
    }

    assert_true(_weight_num < it->second.size());

    return it->second.at(_weight_num);
  };

  // ----------------------------------------------------

  const auto make_mapping_column =
      [this, map_value,
       &col](const size_t _weight_num) -> containers::Column<Float> {
    const auto get_val =
        std::bind(map_value, _weight_num, std::placeholders::_1);

    const auto range = col | VIEWS::transform(get_val);

#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
    const auto ptr = std::make_shared<std::vector<Float>>(
        fct::collect::vector<Float>(range));
#else
    const auto ptr = multithreading_
                         ? std::make_shared<std::vector<Float>>(
                               fct::collect_parallel::vector<Float>(range))
                         : std::make_shared<std::vector<Float>>(
                               fct::collect::vector<Float>(range));
#endif

    const auto name = make_staging_table_colname(col.name(), _weight_num);

    return containers::Column<Float>(ptr, name);
  };

  // ----------------------------------------------------

  if (mapping->size() <= 1) {
    _logger->increment();
    return std::vector<containers::Column<Float>>();
  }

  const auto num_weights = mapping->begin()->second.size();

  const auto iota = fct::iota<size_t>(0, num_weights);

  const auto vec = fct::collect::vector<containers::Column<Float>>(
      iota | VIEWS::transform(make_mapping_column));

  _logger->increment();

  return vec;

  // ----------------------------------------------------
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::make_mapping_columns_text(
    const std::tuple<std::string, std::shared_ptr<const textmining::WordIndex>,
                     MappingForDf::value_type>& _t,
    logging::ProgressLogger* _logger) const {
  // ----------------------------------------------------

  const auto& colname = std::get<0>(_t);

  const auto& word_index = std::get<1>(_t);

  const auto& mapping = std::get<2>(_t);

  assert_true(word_index);

  assert_true(mapping);

  // ----------------------------------------------------

  const auto map_word = [&mapping](const size_t _weight_num,
                                   const Int _word) -> Float {
    const auto it = mapping->find(_word);

    if (it == mapping->end()) {
      return NAN;
    }

    assert_true(_weight_num < it->second.size());

    return it->second.at(_weight_num);
  };

  // ----------------------------------------------------

  const auto map_text_field = [map_word, &word_index](
                                  const size_t _weight_num,
                                  const size_t _i) -> Float {
    const auto map = std::bind(map_word, _weight_num, std::placeholders::_1);

    const auto words = word_index->range(_i);

    auto range = words | VIEWS::transform(map);

    const auto agg = helpers::Aggregations::avg(range.begin(), range.end());

    if (helpers::NullChecker::is_null(agg)) {
      return 0.0;
    }

    return agg;
  };

  // ----------------------------------------------------

  const auto make_mapping_column =
      [this, map_text_field, &colname,
       &word_index](const size_t _weight_num) -> containers::Column<Float> {
    const auto get_val =
        std::bind(map_text_field, _weight_num, std::placeholders::_1);

    const auto iota = fct::iota<size_t>(0, word_index->nrows());

    const auto range = iota | VIEWS::transform(get_val);

#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
    const auto ptr = std::make_shared<std::vector<Float>>(
        fct::collect::vector<Float>(range));
#else
    const auto ptr = multithreading_
                         ? std::make_shared<std::vector<Float>>(
                               fct::collect_parallel::vector<Float>(range))
                         : std::make_shared<std::vector<Float>>(
                               fct::collect::vector<Float>(range));
#endif

    const auto name = make_staging_table_colname(colname, _weight_num);

    return containers::Column<Float>(ptr, name);
  };

  // ----------------------------------------------------

  if (mapping->size() <= 1) {
    _logger->increment();
    return std::vector<containers::Column<Float>>();
  }

  const auto num_weights = mapping->begin()->second.size();

  const auto iota = fct::iota<size_t>(0, num_weights);

  const auto vec = fct::collect::vector<containers::Column<Float>>(
      iota | VIEWS::transform(make_mapping_column));

  _logger->increment();

  return vec;

  // ----------------------------------------------------
}

// ------------------------------------------------------------------------

std::vector<std::pair<Int, Float>> Mapping::make_pairs(
    const Map& _m, const size_t _weight_num) const {
  using Pair = std::pair<Int, Float>;

  auto pairs = std::vector<Pair>();

  for (const auto& p : _m) {
    assert_true(_weight_num < p.second.size());
    pairs.push_back(std::make_pair(p.first, p.second.at(_weight_num)));
  }

  const auto by_value = [](const Pair& _p1, const Pair& _p2) -> bool {
    return _p1.second > _p2.second;
  };

  std::sort(pairs.begin(), pairs.end(), by_value);

  return pairs;
}

// ----------------------------------------------------------------------------

std::map<Int, std::vector<size_t>> Mapping::make_rownum_map_categorical(
    const helpers::Column<Int>& _col) const {
  std::map<Int, std::vector<size_t>> rownum_map;

  for (size_t i = 0; i < _col.nrows_; ++i) {
    const auto key = _col[i];

    if (key < 0) {
      continue;
    }

    const auto it = rownum_map.find(key);

    if (it == rownum_map.end()) {
      rownum_map[key] = {i};
    } else {
      it->second.push_back(i);
    }
  }

  return rownum_map;
}

// ----------------------------------------------------------------------------

std::map<Int, std::vector<size_t>> Mapping::make_rownum_map_discrete(
    const helpers::Column<Float>& _col) const {
  std::map<Int, std::vector<size_t>> rownum_map;

  for (size_t i = 0; i < _col.nrows_; ++i) {
    if (std::isnan(_col[i]) || std::isinf(_col[i])) {
      continue;
    }

    const auto key = static_cast<Int>(_col[i]);

    const auto it = rownum_map.find(key);

    if (it == rownum_map.end()) {
      rownum_map[key] = {i};
    } else {
      it->second.push_back(i);
    }
  }

  return rownum_map;
}

// ----------------------------------------------------------------------------

std::map<Int, std::vector<size_t>> Mapping::make_rownum_map_text(
    const textmining::WordIndex& _word_index) const {
  std::map<Int, std::vector<size_t>> rownum_map;

  for (size_t i = 0; i < _word_index.nrows(); ++i) {
    const auto range = _word_index.range(i);

    const auto unique_words = std::set<Int>(range.begin(), range.end());

    for (const auto key : unique_words) {
      const auto it = rownum_map.find(key);

      if (it == rownum_map.end()) {
        rownum_map[key] = {i};
      } else {
        it->second.push_back(i);
      }
    }
  }

  return rownum_map;
}

// ----------------------------------------------------

typename Mapping::RownumPair Mapping::match_rownums(
    const std::vector<helpers::DataFrame>& _main_tables,
    const std::vector<helpers::DataFrame>& _peripheral_tables,
    const RownumPair& _input) const {
  assert_true(_main_tables.size() == _peripheral_tables.size());

  auto rownums = _input.second;

  for (size_t i = 0; i < _main_tables.size(); ++i) {
    const auto ix = _main_tables.size() - i - 1;

    rownums =
        find_output_ix(rownums, _main_tables.at(ix), _peripheral_tables.at(ix));
  }

  return std::make_pair(_input.first, rownums);
}

// ----------------------------------------------------

MappingAggregation Mapping::parse_aggregation(const std::string& _str) const {
  if (_str == AVG) {
    return MappingAggregation::avg;
  }

  if (_str == COUNT) {
    return MappingAggregation::count;
  }

  if (_str == COUNT_ABOVE_MEAN) {
    return MappingAggregation::count_above_mean;
  }

  if (_str == COUNT_BELOW_MEAN) {
    return MappingAggregation::count_below_mean;
  }

  if (_str == COUNT_DISTINCT) {
    return MappingAggregation::count_distinct;
  }

  if (_str == COUNT_MINUS_COUNT_DISTINCT) {
    return MappingAggregation::count_minus_count_distinct;
  }

  if (_str == COUNT_DISTINCT_OVER_COUNT) {
    return MappingAggregation::count_distinct_over_count;
  }

  if (_str == KURTOSIS) {
    return MappingAggregation::kurtosis;
  }

  if (_str == MAX) {
    return MappingAggregation::max;
  }

  if (_str == MEDIAN) {
    return MappingAggregation::median;
  }

  if (_str == MIN) {
    return MappingAggregation::min;
  }

  if (_str == MODE) {
    return MappingAggregation::mode;
  }

  if (_str == NUM_MAX) {
    return MappingAggregation::num_max;
  }

  if (_str == NUM_MIN) {
    return MappingAggregation::num_min;
  }

  if (_str == Q1) {
    return MappingAggregation::q1;
  }

  if (_str == Q5) {
    return MappingAggregation::q5;
  }

  if (_str == Q10) {
    return MappingAggregation::q10;
  }

  if (_str == Q25) {
    return MappingAggregation::q25;
  }

  if (_str == Q75) {
    return MappingAggregation::q75;
  }

  if (_str == Q90) {
    return MappingAggregation::q90;
  }

  if (_str == Q95) {
    return MappingAggregation::q95;
  }

  if (_str == Q99) {
    return MappingAggregation::q99;
  }

  if (_str == SKEW) {
    return MappingAggregation::skew;
  }

  if (_str == STDDEV) {
    return MappingAggregation::stddev;
  }

  if (_str == SUM) {
    return MappingAggregation::sum;
  }

  if (_str == VAR) {
    return MappingAggregation::var;
  }

  if (_str == VARIATION_COEFFICIENT) {
    return MappingAggregation::variation_coefficient;
  }

  throw_unless(false, "Mapping: Unknown aggregation: '" + _str + "'");

  return MappingAggregation::avg;
}

// ----------------------------------------------------

std::vector<std::string> Mapping::text_columns_to_sql(
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  assert_true(text_names_);

  assert_true(vocabulary_);

  assert_true(peripheral_schema_);

  assert_true(vocabulary_->peripheral().size() == peripheral_schema_->size());

  const auto find_vocabulary = [this]() {
    if (prefix_ == "") {
      return vocabulary_->population_iterators();
    }

    for (size_t i = 0; i < peripheral_schema_->size(); ++i) {
      if (peripheral_schema_->at(i).name_ == table_name_) {
        return vocabulary_->peripheral_iterators().at(i);
      }
    }

    assert_true(false);

    return std::vector<helpers::StringIterator>();
  };

  const auto& vocabulary = find_vocabulary();

  assert_true(text_names_->size() == vocabulary.size());

  assert_true(_sql_dialect_generator);

  const auto mapping_to_sql = [this, &vocabulary, &_sql_dialect_generator](
                                  const size_t _i,
                                  const size_t _weight_num) -> std::string {
    const auto name = _sql_dialect_generator->make_staging_table_colname(
        make_staging_table_colname(text_names_->at(_i), _weight_num));

    return categorical_or_text_column_to_sql(
        vocabulary.at(_i), _sql_dialect_generator,
        transpilation::SQLGenerator::to_upper(name), text_.at(_i), _weight_num,
        true);
  };

  return columns_to_sql(mapping_to_sql, text_, text_names_);
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Mapping::to_json_obj() const {
  assert_true(peripheral_schema_);

  assert_true(population_schema_);

  assert_true(vocabulary_);

  const auto to_json_obj = [](const auto& _obj) { return _obj.to_json_obj(); };

  const auto transform_colnames = [](const Colnames& _colnames) {
    assert_true(_colnames);
    return JSON::vector_to_array_ptr(*_colnames);
  };

  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("type_", type());

  obj->set("aggregation_", JSON::vector_to_array_ptr(aggregation_));

  obj->set("categorical_", transform_mapping(categorical_));

  obj->set("categorical_names_", transform_colnames(categorical_names_));

  obj->set("discrete_", transform_mapping(discrete_));

  obj->set("discrete_names_", transform_colnames(discrete_names_));

  obj->set("min_freq_", min_freq_);

  obj->set("multithreading_", multithreading_);

  obj->set("prefix_", prefix_);

  obj->set("submappings_",
           fct::collect::array(submappings_ | VIEWS::transform(to_json_obj)));

  obj->set("table_name_", table_name_);

  obj->set("text_", transform_mapping(text_));

  obj->set("text_names_", transform_colnames(text_names_));

  /// For reasons for memory efficiency, we do not duplicate the vocabulary.
  if (prefix_ == "") {
    obj->set("peripheral_schema_",
             fct::collect::array(*peripheral_schema_ |
                                 VIEWS::transform(to_json_obj)));

    obj->set("population_schema_", population_schema_->to_json_obj());

    obj->set("vocabulary_", vocabulary_->to_json_obj());
  }

  return obj;
}

// ----------------------------------------------------

std::vector<std::string> Mapping::to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  const auto submapping_to_sql =
      [&_categories, &_sql_dialect_generator](
          const Mapping& _mapping) -> std::vector<std::string> {
    return _mapping.to_sql(_categories, _sql_dialect_generator);
  };

  const auto categorical =
      categorical_columns_to_sql(_categories, _sql_dialect_generator);

  const auto discrete = discrete_columns_to_sql(_sql_dialect_generator);

  const auto text = text_columns_to_sql(_sql_dialect_generator);

  const auto all_submappings =
      submappings_ | VIEWS::transform(submapping_to_sql);

  const auto submappings = fct::join::vector<std::string>(all_submappings);

  return fct::join::vector<std::string>(
      {submappings, categorical, discrete, text});
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Mapping::transform(const TransformParams& _params) const {
  const auto [population, table_holder, _] = build_prerequisites(
      _params.population_df_, _params.peripheral_dfs_, _params.placeholder_,
      _params.peripheral_names_, false);

  const auto num_cols =
      table_holder ? infer_num_cols(*table_holder) : infer_num_cols(population);

  auto logger =
      logging::ProgressLogger("", _params.logger_, num_cols,
                              _params.logging_begin_, _params.logging_end_);

  auto population_df = _params.population_df_;

  auto peripheral_dfs = _params.peripheral_dfs_;

  if (table_holder) {
    transform_peripherals(*table_holder, &logger, &peripheral_dfs);
  }

  transform_data_frame(population, &logger, &population_df);

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

void Mapping::transform_peripherals(
    const helpers::TableHolder& _table_holder, logging::ProgressLogger* _logger,
    std::vector<containers::DataFrame>* _peripheral_dfs) const {
  // ----------------------------------------------------

  const auto find_peripheral =
      [_peripheral_dfs](const Mapping& _mapping) -> containers::DataFrame* {
    for (auto& df : *_peripheral_dfs) {
      if (df.name() == _mapping.table_name_) {
        return &df;
      }
    }

    throw std::runtime_error("Mapping: Data frame '" + _mapping.table_name_ +
                             "' not found!");

    return nullptr;
  };

  // ----------------------------------------------------

  assert_true(submappings_.size() == _table_holder.peripheral_tables().size());

  assert_true(submappings_.size() == _table_holder.subtables().size());

  for (size_t i = 0; i < submappings_.size(); ++i) {
    const auto& mapping = submappings_.at(i);

    const auto subtable = _table_holder.subtables().at(i);

    if (mapping.submappings_.size() > 0) {
      assert_true(subtable);
      mapping.transform_peripherals(subtable.value(), _logger, _peripheral_dfs);
    }

    const auto& immutable = _table_holder.peripheral_tables().at(i);

    const auto df = find_peripheral(mapping);

    mapping.transform_data_frame(immutable, _logger, df);
  }

  // ----------------------------------------------------
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::transform_categorical(
    const containers::DataFrame& _df, logging::ProgressLogger* _logger) const {
  using Pair = std::pair<containers::Column<Int>, MappingForDf::value_type>;

  const auto get_column = [&_df,
                           this](const size_t _i) -> containers::Column<Int> {
    assert_true(categorical_names_);
    assert_true(_i < categorical_names_->size());
    const auto& name = categorical_names_->at(_i);
    return _df.categorical(name);
  };

  const auto get_mapping = [this](const size_t _i) -> MappingForDf::value_type {
    assert_true(_i < categorical_.size());
    return categorical_.at(_i);
  };

  const auto make_pair = [get_column, get_mapping](const size_t _i) -> Pair {
    return std::make_pair(get_column(_i), get_mapping(_i));
  };

  assert_true(categorical_names_);

  assert_true(categorical_.size() == categorical_names_->size());

  const auto make_cols = [this, _logger](const Pair& _p) {
    return make_mapping_columns_int(_p, _logger);
  };

  const auto iota = fct::iota<size_t>(0, categorical_.size());

  const auto range =
      iota | VIEWS::transform(make_pair) | VIEWS::transform(make_cols);

  return fct::join::vector<containers::Column<Float>>(range);
}

// ----------------------------------------------------

void Mapping::transform_data_frame(const helpers::DataFrame& _immutable,
                                   logging::ProgressLogger* _logger,
                                   containers::DataFrame* _data_frame) const {
  const auto add_columns =
      [](const std::vector<containers::Column<Float>>& _cols,
         containers::DataFrame* _df) {
        for (const auto& col : _cols) {
          _df->add_float_column(col, containers::DataFrame::ROLE_NUMERICAL);
        }
      };

  const auto categorical_mappings =
      transform_categorical(*_data_frame, _logger);

  const auto discrete_mappings = transform_discrete(*_data_frame, _logger);

  const auto text_mappings = transform_text(_immutable, *_data_frame, _logger);

  add_columns(categorical_mappings, _data_frame);

  add_columns(discrete_mappings, _data_frame);

  add_columns(text_mappings, _data_frame);
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::transform_discrete(
    const containers::DataFrame& _df, logging::ProgressLogger* _logger) const {
  using Pair = std::pair<containers::Column<Int>, MappingForDf::value_type>;

  const auto cast_as_int = [](const Float _val) -> Int {
    return static_cast<Int>(_val);
  };

  const auto get_column = [&_df, cast_as_int,
                           this](const size_t _i) -> containers::Column<Int> {
    assert_true(discrete_names_);
    assert_true(_i < discrete_names_->size());

    const auto& name = discrete_names_->at(_i);

    const auto col = _df.numerical(name);

    const auto range = col | VIEWS::transform(cast_as_int);

    const auto ptr =
        std::make_shared<std::vector<Int>>(fct::collect::vector<Int>(range));

    return containers::Column<Int>(ptr, col.name());
  };

  const auto get_mapping = [this](const size_t _i) -> MappingForDf::value_type {
    assert_true(_i < discrete_.size());
    return discrete_.at(_i);
  };

  const auto make_pair = [get_column, get_mapping](const size_t _i) -> Pair {
    return std::make_pair(get_column(_i), get_mapping(_i));
  };

  assert_true(discrete_names_);

  assert_true(discrete_.size() == discrete_names_->size());

  const auto make_cols = [this, _logger](const Pair& _p) {
    return make_mapping_columns_int(_p, _logger);
  };

  const auto iota = fct::iota<size_t>(0, discrete_.size());

  const auto range =
      iota | VIEWS::transform(make_pair) | VIEWS::transform(make_cols);

  return fct::join::vector<containers::Column<Float>>(range);
}

// ----------------------------------------------------

Poco::JSON::Array::Ptr Mapping::transform_mapping(
    const MappingForDf& _mapping) const {
  // --------------------------------------------------------------

  const auto map_to_object =
      [](const std::shared_ptr<const std::map<Int, std::vector<Float>>>& _map) {
        assert_true(_map);

        Poco::JSON::Object::Ptr obj(new Poco::JSON::Object());

        for (const auto& [key, value] : *_map) {
          obj->set(std::to_string(key),
                   jsonutils::JSON::vector_to_array_ptr(value));
        }

        return obj;
      };

  // --------------------------------------------------------------

  const auto range = _mapping | VIEWS::transform(map_to_object);

  return fct::collect::array(range);

  // --------------------------------------------------------------
}

// ----------------------------------------------------

std::vector<containers::Column<Float>> Mapping::transform_text(
    const helpers::DataFrame& _immutable, const containers::DataFrame& _df,
    logging::ProgressLogger* _logger) const {
  // --------------------------------------------------------------

  using Pair =
      std::pair<std::string, std::shared_ptr<const textmining::WordIndex>>;

  using Tuple =
      std::tuple<std::string, std::shared_ptr<const textmining::WordIndex>,
                 MappingForDf::value_type>;

  assert_true(_immutable.word_indices_.size() == _immutable.text_.size());

  assert_true(text_names_);

  assert_true(text_.size() == text_names_->size());

  assert_true(text_.size() == _immutable.text_.size());

  // --------------------------------------------------------------

  const auto get_word_index = [this, &_immutable](const size_t _i) -> Pair {
    assert_true(text_names_);
    assert_true(_i < text_names_->size());
    const auto& colname = text_names_->at(_i);
    return std::make_pair(colname, _immutable.word_indices_.at(_i));
  };

  // --------------------------------------------------------------

  const auto get_mapping = [this](const size_t _i) -> MappingForDf::value_type {
    assert_true(_i < text_.size());
    return text_.at(_i);
  };

  // --------------------------------------------------------------

  const auto make_tuple = [get_word_index,
                           get_mapping](const size_t _i) -> Tuple {
    const auto [colname, word_index] = get_word_index(_i);
    return std::make_tuple(colname, word_index, get_mapping(_i));
  };

  // --------------------------------------------------------------

  const auto make_cols = [this, _logger](const Tuple& _t) {
    return make_mapping_columns_text(_t, _logger);
  };

  // --------------------------------------------------------------

  const auto iota = fct::iota<size_t>(0, text_.size());

  const auto range =
      iota | VIEWS::transform(make_tuple) | VIEWS::transform(make_cols);

  return fct::join::vector<containers::Column<Float>>(range);
}

// ----------------------------------------------------

Poco::JSON::Array::Ptr Mapping::transform_text_mapping(
    const TextMapping& _mapping) const {
  // --------------------------------------------------------------

  const auto map_to_object =
      [](const std::map<std::string, std::vector<Float>>& _map) {
        Poco::JSON::Object::Ptr obj(new Poco::JSON::Object());

        for (const auto& [key, value] : _map) {
          obj->set(key, jsonutils::JSON::vector_to_array_ptr(value));
        }

        return obj;
      };

  // --------------------------------------------------------------

  auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  for (const auto& ptr : _mapping) {
    assert_true(ptr);
    arr->add(map_to_object(*ptr));
  }

  return arr;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
