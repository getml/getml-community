// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/to_sql.hpp"

#include <algorithm>
#include <utility>
#include <vector>

#include "containers/containers.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "fct/collect.hpp"
#include "transpilation/SQLGenerator.hpp"

namespace engine {
namespace pipelines {
namespace to_sql {

/// Expresses the feature learners as SQL code.
std::vector<std::string> feature_learners_to_sql(
    const ToSQLParams& _params,
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator);

/// Generates the names of the autofeatures to be included in the transpiled
/// SQL code.
std::vector<std::string> make_autofeature_names(const FittedPipeline& _fitted);

/// Generates the schemata needed for the SQL generation of the staging
/// tables.
std::pair<containers::Schema, std::vector<containers::Schema>>
make_staging_schemata(const FittedPipeline& _fitted);

/// Sometimes features can get excessively long, which makes it hard to
/// display them in the iPython notebook. This takes care of this problem.
std::vector<std::string> overwrite_oversized_features(
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::vector<std::string>& _autofeatures,
    const std::optional<size_t> _size_threshold);

/// Parses the feature name from the code.
std::string parse_feature_name(
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _code);

/// Expresses the preprocessing part as SQL code.
std::vector<std::string> preprocessors_to_sql(
    const ToSQLParams& _params,
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator);

/// Expresses the staging part as SQL code.
std::vector<std::string> staging_to_sql(
    const ToSQLParams& _params,
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator);

// ----------------------------------------------------------------------------

std::vector<std::string> feature_learners_to_sql(
    const ToSQLParams& _params,
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) {
  const auto to_sql = [&_params, &_sql_dialect_generator](
                          const size_t _i) -> std::vector<std::string> {
    const auto& fl = _params.fitted_.feature_learners_.at(_i);

    // TODO: This needs to accept fct::Ref
    const auto all = fl->to_sql(
        _params.categories_, _params.targets_, _params.full_pipeline_,
        _sql_dialect_generator.ptr(), std::to_string(_i + 1) + "_");

    assert_true(all.size() >= fl->num_features());

    const auto num_subfeatures = all.size() - fl->num_features();

    const auto subfeatures =
        fct::collect::vector<std::string>(all | VIEWS::take(num_subfeatures));

    const auto get_feature = [num_subfeatures,
                              all](const size_t _ix) -> std::string {
      assert_true(_ix < all.size());
      return all.at(num_subfeatures + _ix);
    };

    assert_true(_i < _params.fitted_.predictors_.impl_->autofeatures().size());

    const auto& autofeatures =
        _params.fitted_.predictors_.impl_->autofeatures().at(_i);

    const auto features = fct::collect::vector<std::string>(
        autofeatures | VIEWS::transform(get_feature));

    return fct::join::vector<std::string>({subfeatures, features});
  };

  const auto iota =
      fct::iota<size_t>(0, _params.fitted_.feature_learners_.size());

  return fct::join::vector<std::string>(iota | VIEWS::transform(to_sql));
}

// ----------------------------------------------------------------------------

std::vector<std::string> make_autofeature_names(const FittedPipeline& _fitted) {
  const auto to_names =
      [&_fitted](const size_t _i) -> std::vector<std::string> {
    const auto make_name = [_i](const size_t _ix) -> std::string {
      return "feature_" + std::to_string(_i + 1) + "_" +
             std::to_string(_ix + 1);
    };

    assert_true(_i < _fitted.predictors_.impl_->autofeatures().size());

    const auto& autofeatures = _fitted.predictors_.impl_->autofeatures().at(_i);

    return fct::collect::vector<std::string>(autofeatures |
                                             VIEWS::transform(make_name));
  };

  const auto iota = fct::iota<size_t>(0, _fitted.feature_learners_.size());

  return fct::join::vector<std::string>(iota | VIEWS::transform(to_names));
}

// ----------------------------------------------------------------------------

std::pair<containers::Schema, std::vector<containers::Schema>>
make_staging_schemata(const FittedPipeline& _fitted) {
  const auto has_text_field_marker = [](const std::string& _colname) -> bool {
    const auto pos = _colname.find(helpers::Macros::text_field());
    return pos != std::string::npos;
  };

  const auto remove_text_field_marker =
      [](const std::string& _colname) -> std::string {
    const auto pos = _colname.find(helpers::Macros::text_field());
    assert_true(pos != std::string::npos);
    return _colname.substr(0, pos);
  };

  const auto add_text_fields =
      [has_text_field_marker, remove_text_field_marker](
          const containers::Schema& _schema) -> containers::Schema {
    const auto text_fields = fct::collect::vector<std::string>(
        _schema.unused_strings() | VIEWS::filter(has_text_field_marker) |
        VIEWS::transform(remove_text_field_marker));

    const auto text =
        fct::join::vector<std::string>({_schema.text(), text_fields});

    return containers::Schema(
        _schema.named_tuple().replace(fct::make_field<"text_">(text)));
  };

  const auto is_not_text_field = [](const containers::Schema& _schema) -> bool {
    return _schema.name().find(helpers::Macros::text_field()) ==
           std::string::npos;
  };

  const auto staging_schema_population =
      add_text_fields(*_fitted.modified_population_schema_);

  const auto staging_schema_peripheral =
      fct::collect::vector<containers::Schema>(
          *_fitted.modified_peripheral_schema_ |
          VIEWS::filter(is_not_text_field) | VIEWS::transform(add_text_fields));

  return std::make_pair(staging_schema_population, staging_schema_peripheral);
}

// ----------------------------------------------------------------------------

std::string parse_feature_name(
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _code) {
  constexpr const char* FEATURE_NAME = "$FEATURE_NAME_PLACEHOLDER";

  const auto drop_table =
      _sql_dialect_generator->drop_table_if_exists(FEATURE_NAME);

  const auto feature_name_pos = drop_table.find(FEATURE_NAME);

  assert_true(feature_name_pos != std::string::npos);

  const auto drop_table_first_part = drop_table.substr(0, feature_name_pos);

  const auto pos_begin =
      _code.find(drop_table_first_part) + drop_table_first_part.size();

  assert_true(pos_begin != std::string::npos);

  const auto pos_end =
      _code.find(_sql_dialect_generator->quotechar2(), pos_begin);

  assert_true(pos_end != std::string::npos);

  assert_true(pos_end > pos_begin);

  return transpilation::SQLGenerator::to_upper(
      _code.substr(pos_begin, pos_end - pos_begin));
}

// ----------------------------------------------------------------------------

std::vector<std::string> overwrite_oversized_features(
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::vector<std::string>& _features,
    const std::optional<size_t> _size_threshold) {
  if (!_size_threshold) {
    return _features;
  }

  const auto make_feature = [&_sql_dialect_generator, &_features,
                             &_size_threshold](const size_t _i) -> std::string {
    const auto& feature = _features.at(_i);

    if (feature.size() <= *_size_threshold) {
      return feature;
    }

    std::stringstream sql;

    const auto feature_name =
        parse_feature_name(_sql_dialect_generator, feature);

    sql << "-- The size of the SQL code for " << feature_name << " is "
        << feature.size()
        << " characters, which is greater than the size_threshold of "
        << *_size_threshold << "!" << std::endl
        << "-- To display very long features like this anyway, "
        << "increase the size_threshold or "
        << "set the size_threshold to None." << std::endl
        << _sql_dialect_generator->drop_table_if_exists(feature_name)
        << "CREATE TABLE " << _sql_dialect_generator->quotechar1()
        << feature_name << _sql_dialect_generator->quotechar2() << ";"
        << std::endl
        << std::endl
        << std::endl;

    return sql.str();
  };

  const auto iota = fct::iota<size_t>(0, _features.size());

  return fct::collect::vector<std::string>(iota |
                                           VIEWS::transform(make_feature));
}

// ----------------------------------------------------------------------------

std::vector<std::string> preprocessors_to_sql(
    const ToSQLParams& _params,
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) {
  const auto to_sql = [&_params, _sql_dialect_generator](
                          const auto& _p) -> std::vector<std::string> {
    // TODO: This needs to accept fct::Ref
    return _p->to_sql(_params.categories_, _sql_dialect_generator.ptr());
  };
  return fct::join::vector<std::string>(_params.fitted_.preprocessors_ |
                                        VIEWS::transform(to_sql));
}

// ----------------------------------------------------------------------------

std::vector<std::string> staging_to_sql(
    const ToSQLParams& _params,
    const fct::Ref<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator) {
  const auto needs_targets = [](const auto& _f) -> bool {
    return _f->population_needs_targets();
  };

  const auto population_needs_targets =
      _params.targets_ | std::any_of(_params.fitted_.feature_learners_.begin(),
                                     _params.fitted_.feature_learners_.end(),
                                     needs_targets);

  /// TODO: This needs to return fct::Ref.
  const auto [placeholder, peripheral_names] =
      _params.pipeline_.make_placeholder();

  const auto peripheral_needs_targets =
      placeholder->infer_needs_targets(*peripheral_names);

  const auto [staging_schema_population, staging_schema_peripheral] =
      make_staging_schemata(_params.fitted_);

  return _sql_dialect_generator->make_staging_tables(
      population_needs_targets, peripheral_needs_targets,
      staging_schema_population, staging_schema_peripheral);
}

// ----------------------------------------------------------------------------

std::string to_sql(const ToSQLParams& _params) {
  assert_true(_params.fitted_.feature_learners_.size() ==
              _params.fitted_.predictors_.impl_->autofeatures().size());

  const auto sql_dialect_generator =
      transpilation::SQLDialectParser::parse(_params.transpilation_params_);

  const auto staging = _params.full_pipeline_
                           ? staging_to_sql(_params, sql_dialect_generator)
                           : std::vector<std::string>();

  const auto preprocessing =
      _params.full_pipeline_
          ? preprocessors_to_sql(_params, sql_dialect_generator)
          : std::vector<std::string>();

  const auto feature_names = make_autofeature_names(_params.fitted_);

  const auto features = overwrite_oversized_features(
      sql_dialect_generator,
      feature_learners_to_sql(_params, sql_dialect_generator),
      _params.size_threshold_);

  const auto sql =
      fct::join::vector<std::string>({staging, preprocessing, features});

  const auto target_names =
      _params.targets_ ? _params.fitted_.targets() : std::vector<std::string>();

  return sql_dialect_generator->make_sql(
      _params.fitted_.modified_population_schema_->name(), feature_names, sql,
      target_names, _params.fitted_.predictors_.impl_->categorical_colnames(),
      _params.fitted_.predictors_.impl_->numerical_colnames());
}

// ----------------------------------------------------------------------------
}  // namespace to_sql
}  // namespace pipelines
}  // namespace engine
