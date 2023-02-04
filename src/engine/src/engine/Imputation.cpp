// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/Imputation.hpp"

#include "engine/preprocessors/PreprocessorImpl.hpp"

namespace engine {
namespace preprocessors {

void Imputation::add_dummy(const containers::Column<Float>& _original_col,
                           containers::DataFrame* _df) const {
  auto dummy_col =
      containers::Column<Float>(_df->pool(), _original_col.nrows());

  for (size_t i = 0; i < _original_col.nrows(); ++i) {
    if (std::isnan(_original_col[i])) {
      dummy_col[i] = 1.0;
    } else {
      dummy_col[i] = 0.0;
    }
  }

  dummy_col.set_name(make_dummy_name(_original_col.name()));

  _df->add_float_column(dummy_col, containers::DataFrame::ROLE_NUMERICAL);
}

// ----------------------------------------------------

void Imputation::extract_and_add(const std::string& _marker,
                                 const size_t _table,
                                 const containers::Column<Float>& _original_col,
                                 containers::DataFrame* _df) {
  const bool all_nan = std::all_of(_original_col.begin(), _original_col.end(),
                                   [](Float val) { return std::isnan(val); });

  if (all_nan) {
    throw std::runtime_error(
        "Cannot impute column '" + _original_col.name() +
        "'. All of its values are nan. You should set its role to "
        "unused_float.");
  }

  const bool has_inf = std::any_of(_original_col.begin(), _original_col.end(),
                                   [](Float val) { return std::isinf(val); });

  if (has_inf) {
    throw std::runtime_error(
        "Cannot impute column '" + _original_col.name() +
        "'. It contains infinite values. You should set its role to "
        "unused_float.");
  }

  const auto mean =
      utils::Aggregations::avg(_original_col.begin(), _original_col.end());

  const auto any_imputation = impute(_original_col, mean, _df);

  const bool needs_dummy = add_dummies_ && any_imputation;

  if (needs_dummy) {
    add_dummy(_original_col, _df);
  }

  const auto coldesc = helpers::ColumnDescription(
      _marker, std::to_string(_table), _original_col.name());

  cols()[coldesc] = std::make_pair(mean, needs_dummy);
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Imputation::fingerprint() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("type_", type());

  obj->set("add_dummies_", add_dummies_);

  obj->set("dependencies_", JSON::vector_to_array_ptr(dependencies_));

  return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Imputation::fit_transform(const FitParams& _params) {
  const auto population_df = fit_transform_df(
      _params.population_df_, helpers::ColumnDescription::POPULATION, 0);

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs_.size(); ++i) {
    const auto& df = _params.peripheral_dfs_.at(i);

    const auto new_df =
        fit_transform_df(df, helpers::ColumnDescription::PERIPHERAL, i);

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame Imputation::fit_transform_df(
    const containers::DataFrame& _df, const std::string& _marker,
    const size_t _table) {
  const auto blacklist = std::vector<helpers::Subrole>(
      {helpers::Subrole::exclude_preprocessors, helpers::Subrole::email_only,
       helpers::Subrole::substring_only, helpers::Subrole::exclude_imputation});

  auto df = _df;

  for (size_t i = 0; i < _df.num_numericals(); ++i) {
    const auto& original_col = _df.numerical(i);

    if (helpers::SubroleParser::contains_any(original_col.subroles(),
                                             blacklist)) {
      continue;
    }

    extract_and_add(_marker, _table, original_col, &df);
  }

  return df;
}

// ----------------------------------------------------
/*
Imputation Imputation::from_json_obj(const Poco::JSON::Object& _obj) const {
  Imputation that;

  that.add_dummies_ = jsonutils::JSON::get_value<bool>(_obj, "add_dummies_");

  if (_obj.has("column_descriptions_")) {
    const auto column_descriptions =
        jsonutils::JSON::get_object_array(_obj, "column_descriptions_");

    const auto means = jsonutils::JSON::array_to_vector<Float>(
        jsonutils::JSON::get_array(_obj, "means_"));

    const auto needs_dummies = jsonutils::JSON::array_to_vector<bool>(
        jsonutils::JSON::get_array(_obj, "needs_dummies_"));

    assert_true(column_descriptions);

    if (column_descriptions->size() != means.size() ||
        needs_dummies.size() != means.size()) {
      throw std::runtime_error(
          "Could not load Imputation preprocessor. JSON is "
          "poorly "
          "formatted.");
    }

    for (size_t i = 0; i < means.size(); ++i) {
      const auto ptr = column_descriptions->getObject(i);

      assert_true(ptr);

      const auto coldesc = helpers::ColumnDescription(*ptr);

      that.cols()[coldesc] = std::make_pair(means.at(i), needs_dummies.at(i));
    }
  }

  return that;
}
*/
// ----------------------------------------------------

bool Imputation::impute(const containers::Column<Float>& _original_col,
                        const Float _imputation_value,
                        containers::DataFrame* _df) const {
  bool any_imputation = false;

  auto replacement_col =
      containers::Column<Float>(_df->pool(), _original_col.nrows());

  for (size_t i = 0; i < _original_col.nrows(); ++i) {
    if (std::isnan(_original_col[i])) {
      any_imputation = true;
      replacement_col[i] = _imputation_value;
    } else {
      replacement_col[i] = _original_col[i];
    }
  }

  replacement_col.set_name(make_name(_original_col.name(), _imputation_value));

  replacement_col.set_unit(_original_col.unit());

  _df->remove_column(_original_col.name());

  _df->add_float_column(replacement_col, containers::DataFrame::ROLE_NUMERICAL);

  return any_imputation;
}

// ----------------------------------------------------

std::vector<std::pair<Float, bool>> Imputation::retrieve_pairs(
    const std::string& _marker, const size_t _table) const {
  const auto table = std::to_string(_table);

  std::vector<std::pair<Float, bool>> pairs;

  for (const auto& [key, value] : cols()) {
    if (key.marker() == _marker && key.table() == table) {
      pairs.push_back(value);
    }
  }

  return pairs;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Imputation::to_json_obj() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("type_", type());

  obj->set("add_dummies_", add_dummies_);

  auto column_descriptions = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  auto means = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  auto needs_dummies = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  for (const auto& [key, value] : cols()) {
    column_descriptions->add(key.to_json_obj());
    means->add(value.first);
    needs_dummies->add(value.second);
  }

  obj->set("column_descriptions_", column_descriptions);

  obj->set("means_", means);

  obj->set("needs_dummies_", needs_dummies);

  return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Imputation::transform(const TransformParams& _params) const {
  const auto population_df = transform_df(
      _params.population_df_, helpers::ColumnDescription::POPULATION, 0);

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs_.size(); ++i) {
    const auto& df = _params.peripheral_dfs_.at(i);

    const auto new_df =
        transform_df(df, helpers::ColumnDescription::PERIPHERAL, i);

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame Imputation::transform_df(const containers::DataFrame& _df,
                                               const std::string& _marker,
                                               const size_t _table) const {
  auto df = _df;

  const auto all_cols = get_all_cols();

  const auto names =
      PreprocessorImpl::retrieve_names(_marker, _table, all_cols);

  const auto pairs = retrieve_pairs(_marker, _table);

  assert_true(names.size() == pairs.size());

  for (size_t i = 0; i < names.size(); ++i) {
    const auto& name = names.at(i);

    const auto mean = pairs.at(i).first;

    const bool needs_dummy = pairs.at(i).second;

    const auto original_col = _df.numerical(name);

    impute(original_col, mean, &df);

    if (needs_dummy) {
      add_dummy(original_col, &df);
    }
  }

  return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
