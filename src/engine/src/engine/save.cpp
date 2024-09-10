// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/save.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/from_named_tuple.hpp>
#include <string>
#include <vector>

#include "engine/pipelines/PipelineJSON.hpp"
#include "engine/pipelines/ToSQLParams.hpp"
#include "engine/pipelines/to_sql.hpp"
#include "engine/utils/SQLDependencyTracker.hpp"
#include "helpers/Saver.hpp"

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
  auto tfile = Poco::TemporaryFile(_params.temp_dir());

  tfile.createDirectories();

  save_preprocessors(_params, tfile);

  save_feature_learners(_params, tfile);

  save_pipeline_json(_params, tfile);

  const auto format = _params.format();

  helpers::Saver::save(tfile.path() + "/obj", _params.pipeline().obj(), format);

  _params.pipeline().scores().save(tfile.path() + "/scores", format);

  _params.fitted().feature_selectors_.impl_->save(
      tfile.path() + "/feature-selector-impl", format);

  _params.fitted().predictors_.impl_->save(tfile.path() + "/predictor-impl",
                                           format);

  save_predictors(_params.fitted().feature_selectors_.predictors_,
                  "feature-selector", tfile, format);

  save_predictors(_params.fitted().predictors_.predictors_, "predictor", tfile,
                  format);

  using DialectType = typename transpilation::TranspilationParams::DialectType;

  const auto transpilation_params = transpilation::TranspilationParams{
      .dialect = DialectType::make<"human-readable sql">(),
      .nchar_categorical = 128,
      .nchar_join_key = 128,
      .nchar_text = 4096,
      .schema = ""};

  const auto to_sql_params = rfl::from_named_tuple<ToSQLParams>(
      rfl::to_named_tuple(_params) *
      rfl::make_field<"size_threshold_", std::optional<size_t>>(std::nullopt) *
      rfl::make_field<"full_pipeline_">(true) *
      rfl::make_field<"targets_">(true) *
      rfl::make_field<"transpilation_params_">(transpilation_params));

  const auto sql_code = to_sql::to_sql(to_sql_params);

  utils::SQLDependencyTracker(tfile.path() + "/SQL/")
      .save_dependencies(sql_code);

  move_tfile(_params.path(), _params.name(), &tfile);
}

// ----------------------------------------------------------------------------

void save_feature_learners(const SaveParams& _params,
                           const Poco::TemporaryFile& _tfile) {
  const auto format = _params.format();
  for (size_t i = 0; i < _params.fitted().feature_learners_.size(); ++i) {
    const auto& fl = _params.fitted().feature_learners_.at(i);
    fl->save(_tfile.path() + "/feature-learner-" + std::to_string(i), format);
  }
}

// ----------------------------------------------------------------------------

void save_pipeline_json(const SaveParams& _params,
                        const Poco::TemporaryFile& _tfile) {
  const auto& p = _params.pipeline();

  const auto& f = _params.fitted();

  const auto pipeline_json =
      PipelineJSON{.fingerprints = f.fingerprints_,
                   .allow_http = p.allow_http(),
                   .creation_time = p.creation_time(),
                   .modified_peripheral_schema = f.modified_peripheral_schema_,
                   .modified_population_schema = f.modified_population_schema_,
                   .peripheral_schema = f.peripheral_schema_,
                   .population_schema = f.population_schema_,
                   .targets = f.targets()};

  helpers::Saver::save(_tfile.path() + "/pipeline", pipeline_json,
                       _params.format());
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
  const auto format = _params.format();
  for (size_t i = 0; i < _params.fitted().preprocessors_.size(); ++i) {
    const auto& p = _params.fitted().preprocessors_.at(i);
    p->save(_tfile.path() + "/preprocessor-" + std::to_string(i), format);
  }
}

// ----------------------------------------------------------------------------
}  // namespace save
}  // namespace pipelines
}  // namespace engine
