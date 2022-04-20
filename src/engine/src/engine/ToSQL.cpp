#include "engine/pipelines/ToSQL.hpp"

#include <algorithm>

namespace engine {
namespace pipelines {

std::vector<std::string> ToSQL::feature_learners_to_sql(
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

    assert_true(_i < _params.fitted_.predictor_impl_->autofeatures().size());

    const auto& autofeatures =
        _params.fitted_.predictor_impl_->autofeatures().at(_i);

    const auto features = fct::collect::vector<std::string>(
        autofeatures | VIEWS::transform(get_feature));

    return fct::join::vector<std::string>({subfeatures, features});
  };

  const auto iota =
      fct::iota<size_t>(0, _params.fitted_.feature_learners_.size());

  return fct::join::vector<std::string>(iota | VIEWS::transform(to_sql));
}

// ----------------------------------------------------------------------------

std::vector<std::string> ToSQL::make_autofeature_names(
    const FittedPipeline& _fitted) {
  const auto to_names =
      [&_fitted](const size_t _i) -> std::vector<std::string> {
    const auto make_name = [_i](const size_t _ix) -> std::string {
      return "feature_" + std::to_string(_i + 1) + "_" +
             std::to_string(_ix + 1);
    };

    assert_true(_i < _fitted.predictor_impl_->autofeatures().size());

    const auto& autofeatures = _fitted.predictor_impl_->autofeatures().at(_i);

    return fct::collect::vector<std::string>(autofeatures |
                                             VIEWS::transform(make_name));
  };

  const auto iota = fct::iota<size_t>(0, _fitted.feature_learners_.size());

  return fct::join::vector<std::string>(iota | VIEWS::transform(to_names));
}

// ----------------------------------------------------------------------------

std::pair<containers::Schema, std::vector<containers::Schema>>
ToSQL::make_staging_schemata(const FittedPipeline& _fitted) {
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
        _schema.unused_strings_ | VIEWS::filter(has_text_field_marker) |
        VIEWS::transform(remove_text_field_marker));

    return containers::Schema{
        .categoricals_ = _schema.categoricals_,
        .discretes_ = _schema.discretes_,
        .join_keys_ = _schema.join_keys_,
        .name_ = _schema.name_,
        .numericals_ = _schema.numericals_,
        .targets_ = _schema.targets_,
        .text_ = fct::join::vector<std::string>({_schema.text_, text_fields}),
        .time_stamps_ = _schema.time_stamps_,
        .unused_floats_ = _schema.unused_floats_,
        .unused_strings_ = _schema.unused_strings_};
  };

  const auto is_not_text_field = [](const containers::Schema& _schema) -> bool {
    return _schema.name_.find(helpers::Macros::text_field()) ==
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

std::vector<std::string> ToSQL::preprocessors_to_sql(
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

std::vector<std::string> ToSQL::staging_to_sql(
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

  assert_true(placeholder);

  assert_true(peripheral_names);

  const auto peripheral_needs_targets =
      placeholder->infer_needs_targets(*peripheral_names);

  const auto [staging_schema_population, staging_schema_peripheral] =
      make_staging_schemata(_params.fitted_);

  return _sql_dialect_generator->make_staging_tables(
      population_needs_targets, peripheral_needs_targets,
      staging_schema_population, staging_schema_peripheral);
}

// ----------------------------------------------------------------------------

std::string ToSQL::to_sql(const ToSQLParams& _params) {
  assert_true(_params.fitted_.feature_learners_.size() ==
              _params.fitted_.predictor_impl_->autofeatures().size());

  // TODO: This needs to return fct::Ref.
  const auto sql_dialect_generator =
      fct::Ref<const transpilation::SQLDialectGenerator>(
          transpilation::SQLDialectParser::parse(
              _params.transpilation_params_));

  const auto staging = _params.full_pipeline_
                           ? staging_to_sql(_params, sql_dialect_generator)
                           : std::vector<std::string>();

  const auto preprocessing =
      _params.full_pipeline_
          ? preprocessors_to_sql(_params, sql_dialect_generator)
          : std::vector<std::string>();

  const auto features = feature_learners_to_sql(_params, sql_dialect_generator);

  const auto sql =
      fct::join::vector<std::string>({staging, preprocessing, features});

  const auto autofeatures = make_autofeature_names(_params.fitted_);

  const auto target_names =
      _params.targets_ ? _params.fitted_.targets_ : std::vector<std::string>();

  return sql_dialect_generator->make_sql(
      _params.fitted_.modified_population_schema_->name_, autofeatures, sql,
      target_names, _params.fitted_.predictor_impl_->categorical_colnames(),
      _params.fitted_.predictor_impl_->numerical_colnames());
}

// ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine
