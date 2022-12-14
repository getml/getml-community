// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/preprocessors/CategoryTrimmer.hpp"

// ----------------------------------------------------

#include "engine/containers/Column.hpp"
#include "engine/containers/DataFrame.hpp"
#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "fct/IotaRange.hpp"
#include "fct/collect.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/NullChecker.hpp"
#include "transpilation/SQLGenerator.hpp"

// ----------------------------------------------------

namespace engine {
namespace preprocessors {

// ----------------------------------------------------

typename CategoryTrimmer::CategoryPair CategoryTrimmer::category_pair_from_obj(
    const Poco::JSON::Object::Ptr& _ptr) const {
  assert_true(_ptr);

  throw_unless(_ptr, "CategoryPair: Not an object.");

  const auto first = helpers::ColumnDescription(
      *JSON::get_object(*_ptr, "column_description_"));

  const auto vec = JSON::array_to_vector<Int>(JSON::get_array(*_ptr, "set_"));

  const auto second = fct::Ref<std::set<Int>>::make(vec.begin(), vec.end());

  return std::make_pair(first, second);
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr CategoryTrimmer::category_pair_to_obj(
    const CategoryPair& _pair) const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("column_description_", _pair.first.to_json_obj());

  obj->set("set_", JSON::vector_to_array_ptr(std::vector<Int>(
                       _pair.second->begin(), _pair.second->end())));

  return obj;
}

// ----------------------------------------------------

std::string CategoryTrimmer::column_to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const CategoryPair& _pair) const {
  assert_true(_sql_dialect_generator);

  const auto staging_table =
      transpilation::SQLGenerator::make_staging_table_name(_pair.first.table_);

  const auto colname =
      _sql_dialect_generator->make_staging_table_colname(_pair.first.name_);

  std::stringstream sql;

  sql << _sql_dialect_generator->trimming()->make_header(staging_table,
                                                         colname);

  const auto vec = std::vector<Int>(_pair.second->begin(), _pair.second->end());

  constexpr size_t batch_size = 500;

  for (size_t batch_begin = 0; batch_begin < _pair.second->size();
       batch_begin += batch_size) {
    const auto batch_end =
        std::min(_pair.second->size(), batch_begin + batch_size);

    sql << _sql_dialect_generator->trimming()->make_insert_into(staging_table,
                                                                colname);

    for (size_t i = batch_begin; i < batch_end; ++i) {
      const std::string begin = (i == batch_begin) ? "" : "      ";

      const auto val = vec.at(i);

      assert_true(val >= 0);

      assert_true(static_cast<size_t>(val) < _categories.size());

      const auto end = (i == batch_end - 1) ? ";\n\n" : ",\n";

      sql << begin << "( '" << _categories.at(val).str() << "' )" << end;
    }
  }

  assert_true(_sql_dialect_generator);

  sql << _sql_dialect_generator->trimming()->join(staging_table, colname,
                                                  TRIMMED);

  return sql.str();
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr CategoryTrimmer::fingerprint() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("dependencies_", JSON::vector_to_array_ptr(dependencies_));

  obj->set("max_num_categories_", max_num_categories_);

  obj->set("min_freq_", min_freq_);

  obj->set("type_", type());

  return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
CategoryTrimmer::fit_transform(const FitParams& _params) {
  const auto fit_peripheral = [this](const auto& _df) {
    return fit_df(_df, helpers::ColumnDescription::PERIPHERAL);
  };

  population_sets_ =
      fit_df(_params.population_df_, helpers::ColumnDescription::POPULATION);

  peripheral_sets_ = fct::collect::vector<std::vector<CategoryPair>>(
      _params.peripheral_dfs_ | VIEWS::transform(fit_peripheral));

  (*_params.categories_)[strings::String(TRIMMED)];

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

std::vector<typename CategoryTrimmer::CategoryPair> CategoryTrimmer::fit_df(
    const containers::DataFrame& _df, const std::string& _marker) const {
  const auto include = [](const auto& _col) -> bool {
    const auto blacklist = std::vector<helpers::Subrole>(
        {helpers::Subrole::exclude_preprocessors, helpers::Subrole::email_only,
         helpers::Subrole::substring_only,
         helpers::Subrole::exclude_category_trimmer});
    return !helpers::SubroleParser::contains_any(_col.subroles(), blacklist);
  };

  const auto to_column_description = [&_df, &_marker](const auto& _col) {
    return helpers::ColumnDescription(_marker, _df.name(), _col.name());
  };

  const auto to_set = [this](const auto& _col) {
    return make_category_set(_col);
  };

  const auto to_pair = [to_column_description,
                        to_set](const auto& _col) -> CategoryPair {
    return std::make_pair(to_column_description(_col), to_set(_col));
  };

  const auto range =
      _df.categoricals() | VIEWS::filter(include) | VIEWS::transform(to_pair);

  return fct::collect::vector<CategoryPair>(range);
}

// ----------------------------------------------------

fct::Ref<const std::set<Int>> CategoryTrimmer::make_category_set(
    const containers::Column<Int>& _col) const {
  using Pair = std::pair<Int, size_t>;

  const auto counts = make_counts(_col);

  const auto count_greater_than_min_freq = [this](const Pair& p) -> bool {
    return p.second >= min_freq_;
  };

  const auto get_first = [](const Pair& p) -> Int { return p.first; };

  const auto range = counts | VIEWS::filter(count_greater_than_min_freq) |
                     VIEWS::transform(get_first) |
                     VIEWS::take(max_num_categories_);

  return fct::Ref<const std::set<Int>>::make(fct::collect::set<Int>(range));
}

// ----------------------------------------------------

std::vector<std::pair<Int, size_t>> CategoryTrimmer::make_counts(
    const containers::Column<Int>& _col) const {
  const auto count_map = make_map(_col);

  using Pair = std::pair<Int, size_t>;

  auto count_vec = std::vector<Pair>(count_map.begin(), count_map.end());

  const auto by_count = [](const Pair& p1, const Pair& p2) -> bool {
    return p1.second > p2.second;
  };

  std::sort(count_vec.begin(), count_vec.end(), by_count);

  return count_vec;
}

// ----------------------------------------------------

std::map<Int, size_t> CategoryTrimmer::make_map(
    const containers::Column<Int>& _col) const {
  std::map<Int, size_t> count_map;

  for (const auto val : _col) {
    if (helpers::NullChecker::is_null(val)) {
      continue;
    }

    const auto it = count_map.find(val);

    if (it == count_map.end()) {
      count_map[val] = 1;
    } else {
      it->second++;
    }
  }

  return count_map;
}

// ----------------------------------------------------

CategoryTrimmer CategoryTrimmer::from_json_obj(
    const Poco::JSON::Object& _obj) const {
  CategoryTrimmer that;

  that.max_num_categories_ =
      JSON::get_value<size_t>(_obj, "max_num_categories_");

  that.min_freq_ = JSON::get_value<size_t>(_obj, "min_freq_");

  if (!_obj.has("population_sets_")) {
    return that;
  }

  const auto population_sets_arr = JSON::get_array(_obj, "population_sets_");

  for (size_t i = 0; i < population_sets_arr->size(); ++i) {
    that.population_sets_.push_back(
        category_pair_from_obj(population_sets_arr->getObject(i)));
  }
  const auto peripheral_sets_arr = JSON::get_array(_obj, "peripheral_sets_");

  for (size_t i = 0; i < peripheral_sets_arr->size(); ++i) {
    const auto& arr = peripheral_sets_arr->getArray(i);

    throw_unless(arr, "CategoryTrimmer: Not an array!");

    auto vec = std::vector<CategoryPair>();

    for (size_t j = 0; j < arr->size(); ++j) {
      vec.push_back(category_pair_from_obj(arr->getObject(j)));
    }

    that.peripheral_sets_.push_back(vec);
  }

  return that;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr CategoryTrimmer::to_json_obj() const {
  const auto to_obj = [this](const auto& _pair) -> Poco::JSON::Object::Ptr {
    return category_pair_to_obj(_pair);
  };

  const auto to_arr = [to_obj](const auto& _vec) -> Poco::JSON::Array::Ptr {
    return fct::collect::array(_vec | VIEWS::transform(to_obj));
  };

  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("dependencies_", JSON::vector_to_array_ptr(dependencies_));

  obj->set("max_num_categories_", max_num_categories_);

  obj->set("min_freq_", min_freq_);

  obj->set("peripheral_sets_",
           fct::collect::array(peripheral_sets_ | VIEWS::transform(to_arr)));

  obj->set("population_sets_",
           fct::collect::array(population_sets_ | VIEWS::transform(to_obj)));

  obj->set("type_", type());

  return obj;
}

// ----------------------------------------------------

std::vector<std::string> CategoryTrimmer::to_sql(
    const helpers::StringIterator& _categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) const {
  assert_true(_sql_dialect_generator);

  std::vector<std::string> sql_code;

  for (const auto& pair : population_sets_) {
    sql_code.push_back(
        column_to_sql(_categories, _sql_dialect_generator, pair));
  }

  for (const auto& vec : peripheral_sets_) {
    for (const auto& pair : vec) {
      sql_code.push_back(
          column_to_sql(_categories, _sql_dialect_generator, pair));
    }
  }

  return sql_code;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
CategoryTrimmer::transform(const TransformParams& _params) const {
  assert_true(peripheral_sets_.size() == _params.peripheral_dfs_.size());

  const auto pool = _params.population_df_.pool()
                        ? std::make_shared<memmap::Pool>(
                              _params.population_df_.pool()->temp_dir())
                        : std::shared_ptr<memmap::Pool>();

  const auto population_df = transform_df(
      population_sets_, pool, _params.categories_, _params.population_df_);

  const auto make_peripheral_df =
      [this, &_params, pool](const size_t _i) -> containers::DataFrame {
    return transform_df(peripheral_sets_.at(_i), pool, _params.categories_,
                        _params.peripheral_dfs_.at(_i));
  };

  const auto iota = fct::IotaRange<size_t>(0, peripheral_sets_.size());

  const auto peripheral_dfs = fct::collect::vector<containers::DataFrame>(
      iota | VIEWS::transform(make_peripheral_df));

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame CategoryTrimmer::transform_df(
    const std::vector<CategoryPair>& _sets,
    const std::shared_ptr<memmap::Pool>& _pool,
    const fct::Ref<const containers::Encoding>& _categories,
    const containers::DataFrame& _df) const {
  using Set = CategoryPair::second_type;

  using ColumnAndSet = std::pair<containers::Column<Int>, Set>;

  const auto trimmed = (*_categories)[strings::String(TRIMMED)];

  const auto get_col = [&_df](const auto& _pair) -> ColumnAndSet {
    return std::make_pair(_df.categorical(_pair.first.name_), _pair.second);
  };

  const auto trim_value = [trimmed](const Int _value, const Set& _set) -> Int {
    if (_set->find(_value) == _set->end()) {
      return trimmed;
    }
    return _value;
  };

  const auto make_trimmed_column =
      [_pool,
       trim_value](const ColumnAndSet& _pair) -> containers::Column<Int> {
    const auto& orig_col = _pair.first;

    auto new_col = containers::Column<Int>(_pool, orig_col.nrows());

    new_col.set_name(orig_col.name());
    new_col.set_subroles(orig_col.subroles());
    new_col.set_unit(orig_col.unit());

    for (size_t i = 0; i < new_col.nrows(); ++i) {
      new_col[i] = trim_value(orig_col[i], _pair.second);
    }

    return new_col;
  };

  const auto trimmed_columns =
      _sets | VIEWS::transform(get_col) | VIEWS::transform(make_trimmed_column);

  auto df = _df;

  for (const auto col : trimmed_columns) {
    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
