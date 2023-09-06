// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/save.hpp"

#include <string>
#include <vector>

#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/ToSQLParams.hpp"
#include "engine/pipelines/to_sql.hpp"
#include "engine/utils/SQLDependencyTracker.hpp"
#include "flexbuffers/from_flexbuffers.hpp"
#include "flexbuffers/to_flexbuffers.hpp"
#include "helpers/Saver.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"

namespace engine {
namespace pipelines {
namespace save {

/// Saves the feature learners.
void save_feature_learners(const SaveParams& _params,
                           const Poco::TemporaryFile& _tfile);

/// Saves the pipeline itself to a JSON file.
void save_pipeline_json(const SaveParams& _params,
                        const Poco::TemporaryFile& _tfile);

/// Saves the feature selectors or predictors.
void save_predictors(
    const std::vector<std::vector<rfl::Ref<const predictors::Predictor>>>&
        _predictors,
    const std::string& _purpose, const Poco::TemporaryFile& _tfile,
    const typename helpers::Saver::Format& _format);

/// Saves the preprocessors.
void save_preprocessors(const SaveParams& _params,
                        const Poco::TemporaryFile& _tfile);

/// Moves the files from their temporary location to their final destination.
void move_tfile(const std::string& _path, const std::string& _name,
                Poco::TemporaryFile* _tfile);

// ----------------------------------------------------------------------------

void move_tfile(const std::string& _path, const std::string& _name,
                Poco::TemporaryFile* _tfile) {
  auto file = Poco::File(_path + _name);

  file.createDirectories();

  file.remove(true);

  _tfile->renameTo(file.path());

  _tfile->keep();
}

// ----------------------------------------------------------------------------

void save(const SaveParams& _params) {
  auto tfile = Poco::TemporaryFile(_params.get<"temp_dir_">());

  tfile.createDirectories();

  save_preprocessors(_params, tfile);

  save_feature_learners(_params, tfile);

  save_pipeline_json(_params, tfile);

  const auto format = _params.get<"format_">();

  helpers::Saver::save(tfile.path() + "/obj", _params.get<"pipeline_">().obj(),
                       format);

  _params.get<"pipeline_">().scores().save(tfile.path() + "/scores", format);

  _params.get<"fitted_">().feature_selectors_.impl_->save(
      tfile.path() + "/feature-selector-impl", format);

  _params.get<"fitted_">().predictors_.impl_->save(
      tfile.path() + "/predictor-impl", format);

  save_predictors(_params.get<"fitted_">().feature_selectors_.predictors_,
                  "feature-selector", tfile, format);

  save_predictors(_params.get<"fitted_">().predictors_.predictors_, "predictor",
                  tfile, format);

  using DialectType = typename transpilation::TranspilationParams::DialectType;

  const auto transpilation_params = transpilation::TranspilationParams(
      rfl::make_field<"dialect_">(DialectType::make<"human-readable sql">()) *
      rfl::Field<"nchar_categorical_", size_t>(128) *
      rfl::Field<"nchar_join_key_", size_t>(128) *
      rfl::Field<"nchar_text_", size_t>(4096) *
      rfl::Field<"schema_", std::string>(""));

  const auto to_sql_params = ToSQLParams(
      _params *
      rfl::make_field<"size_threshold_", std::optional<size_t>>(std::nullopt) *
      rfl::make_field<"full_pipeline_">(true) *
      rfl::make_field<"targets_">(true) *
      rfl::make_field<"transpilation_params_">(transpilation_params));

  const auto sql_code = to_sql::to_sql(to_sql_params);

  utils::SQLDependencyTracker(tfile.path() + "/SQL/")
      .save_dependencies(sql_code);

  move_tfile(_params.get<"path_">(), _params.get<"name_">(), &tfile);
}

// ----------------------------------------------------------------------------

void save_feature_learners(const SaveParams& _params,
                           const Poco::TemporaryFile& _tfile) {
  const auto format = _params.get<"format_">();
  for (size_t i = 0; i < _params.get<"fitted_">().feature_learners_.size();
       ++i) {
    const auto& fl = _params.get<"fitted_">().feature_learners_.at(i);
    fl->save(_tfile.path() + "/feature-learner-" + std::to_string(i), format);
  }
}

// ----------------------------------------------------------------------------

void save_pipeline_json(const SaveParams& _params,
                        const Poco::TemporaryFile& _tfile) {
  const auto& p = _params.get<"pipeline_">();

  const auto& f = _params.get<"fitted_">();

  const PipelineJSON pipeline_json =
      f.fingerprints_ * rfl::make_field<"allow_http_">(p.allow_http()) *
      rfl::make_field<"creation_time_">(p.creation_time()) *
      rfl::make_field<"modified_peripheral_schema_">(
          f.modified_peripheral_schema_) *
      rfl::make_field<"modified_population_schema_">(
          f.modified_population_schema_) *
      rfl::make_field<"peripheral_schema_">(f.peripheral_schema_) *
      rfl::make_field<"population_schema_">(f.population_schema_) *
      rfl::make_field<"targets_">(f.targets());

  helpers::Saver::save(_tfile.path() + "/pipeline", pipeline_json,
                       _params.get<"format_">());
}

// ----------------------------------------------------------------------------

void save_predictors(
    const std::vector<std::vector<rfl::Ref<const predictors::Predictor>>>&
        _predictors,
    const std::string& _purpose, const Poco::TemporaryFile& _tfile,
    const typename helpers::Saver::Format& _format) {
  for (size_t i = 0; i < _predictors.size(); ++i) {
    for (size_t j = 0; j < _predictors.at(i).size(); ++j) {
      const auto& p = _predictors.at(i).at(j);
      p->save(_tfile.path() + "/" + _purpose + "-" + std::to_string(i) + "-" +
                  std::to_string(j),
              _format);
    }
  }
}

// ----------------------------------------------------------------------------

void save_preprocessors(const SaveParams& _params,
                        const Poco::TemporaryFile& _tfile) {
  const auto format = _params.get<"format_">();
  for (size_t i = 0; i < _params.get<"fitted_">().preprocessors_.size(); ++i) {
    const auto& p = _params.get<"fitted_">().preprocessors_.at(i);
    p->save(_tfile.path() + "/preprocessor-" + std::to_string(i), format);
  }
}

// ----------------------------------------------------------------------------
}  // namespace save
}  // namespace pipelines
}  // namespace engine
