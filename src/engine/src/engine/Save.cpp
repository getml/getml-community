// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Save.hpp"

#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/ToSQL.hpp"
#include "engine/pipelines/ToSQLParams.hpp"
#include "helpers/Saver.hpp"

namespace engine {
namespace pipelines {

void Save::move_tfile(const std::string& _path, const std::string& _name,
                      Poco::TemporaryFile* _tfile) {
  auto file = Poco::File(_path + _name);

  file.createDirectories();

  file.remove(true);

  _tfile->renameTo(file.path());

  _tfile->keep();
}

// ----------------------------------------------------------------------------

void Save::save(const SaveParams& _params) {
  auto tfile = Poco::TemporaryFile(_params.temp_dir_);

  tfile.createDirectories();

  save_preprocessors(_params, tfile);

  save_feature_learners(_params, tfile);

  save_pipeline_json(_params, tfile);

  helpers::Saver::save_as_json(tfile.path() + "/obj.json",
                               _params.pipeline_.obj());

  _params.pipeline_.scores().save(tfile.path() + "/scores.json");

  _params.fitted_.feature_selectors_.impl_->save(tfile.path() +
                                                 "/feature-selector-impl.json");

  _params.fitted_.predictors_.impl_->save(tfile.path() +
                                          "/predictor-impl.json");

  save_predictors(_params.fitted_.feature_selectors_.predictors_,
                  "feature-selector", tfile);

  save_predictors(_params.fitted_.predictors_.predictors_, "predictor", tfile);

  const auto transpilation_params = transpilation::TranspilationParams{
      .dialect_ = transpilation::SQLDialectParser::HUMAN_READABLE_SQL,
      .nchar_categorical_ = 128,
      .nchar_join_key_ = 128,
      .nchar_text_ = 4096,
      .schema_ = ""};

  const auto to_sql_params =
      ToSQLParams{.categories_ = _params.categories_,
                  .fitted_ = _params.fitted_,
                  .full_pipeline_ = true,
                  .pipeline_ = _params.pipeline_,
                  .targets_ = true,
                  .transpilation_params_ = transpilation_params};

  const auto sql_code = ToSQL::to_sql(to_sql_params);

  utils::SQLDependencyTracker(tfile.path() + "/SQL/")
      .save_dependencies(sql_code);

  move_tfile(_params.path_, _params.name_, &tfile);
}

// ----------------------------------------------------------------------------

void Save::save_feature_learners(const SaveParams& _params,
                                 const Poco::TemporaryFile& _tfile) {
  for (size_t i = 0; i < _params.fitted_.feature_learners_.size(); ++i) {
    const auto& fe = _params.fitted_.feature_learners_.at(i);
    fe->save(_tfile.path() + "/feature-learner-" + std::to_string(i) + ".json");
  }
}

// ----------------------------------------------------------------------------

void Save::save_pipeline_json(const SaveParams& _params,
                              const Poco::TemporaryFile& _tfile) {
  const auto to_obj = [](const helpers::Schema& s) { return s.to_json_obj(); };

  const auto& p = _params.pipeline_;

  const auto& f = _params.fitted_;

  const PipelineJSON pipeline_json =
      f.fingerprints_ * fct::make_field<"allow_http_">(p.allow_http()) *
      fct::make_field<"creation_time_">(p.creation_time()) *
      fct::make_field<"modified_peripheral_schema_">(
          f.modified_peripheral_schema_) *
      fct::make_field<"modified_population_schema_">(
          f.modified_population_schema_) *
      fct::make_field<"peripheral_schema_">(f.peripheral_schema_) *
      fct::make_field<"population_schema_">(f.population_schema_) *
      fct::make_field<"targets_">(f.targets());

  helpers::Saver::save_as_json(_tfile.path() + "/pipeline.json", pipeline_json);
}

// ----------------------------------------------------------------------------

void Save::save_predictors(
    const std::vector<std::vector<fct::Ref<const predictors::Predictor>>>&
        _predictors,
    const std::string& _purpose, const Poco::TemporaryFile& _tfile) {
  for (size_t i = 0; i < _predictors.size(); ++i) {
    for (size_t j = 0; j < _predictors.at(i).size(); ++j) {
      const auto& p = _predictors.at(i).at(j);
      p->save(_tfile.path() + "/" + _purpose + "-" + std::to_string(i) + "-" +
              std::to_string(j));
    }
  }
}

// ----------------------------------------------------------------------------

void Save::save_preprocessors(const SaveParams& _params,
                              const Poco::TemporaryFile& _tfile) {
  for (size_t i = 0; i < _params.fitted_.preprocessors_.size(); ++i) {
    const auto& p = _params.fitted_.preprocessors_.at(i);
    p->save(_tfile.path() + "/preprocessor-" + std::to_string(i) + ".json");
  }
}

// ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine
