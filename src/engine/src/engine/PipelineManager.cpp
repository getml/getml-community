// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/PipelineManager.hpp"

#include <filesystem>
#include <ranges>
#include <stdexcept>

#include "commands/DataFramesOrViews.hpp"
#include "commands/ProjectCommand.hpp"
#include "containers/Roles.hpp"
#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/pipelines/ToSQLParams.hpp"
#include "engine/pipelines/load_fitted.hpp"
#include "engine/pipelines/pipelines.hpp"
#include "engine/pipelines/to_sql.hpp"
#include "fct/Field.hpp"
#include "fct/always_false.hpp"
#include "fct/make_named_tuple.hpp"
#include "transpilation/TranspilationParams.hpp"
#include "transpilation/transpilation.hpp"

namespace engine {
namespace handlers {

void PipelineManager::add_features_to_df(
    const pipelines::FittedPipeline& _fitted,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    containers::DataFrame* _df) const {
  const auto [autofeatures, numerical, categorical] = _fitted.feature_names();

  assert_true(autofeatures.size() + numerical.size() ==
              _numerical_features.size());

  size_t j = 0;

  for (size_t i = 0; i < autofeatures.size(); ++i) {
    auto col = containers::Column<Float>(_numerical_features.at(j++).ptr());
    col.set_name(autofeatures.at(i));
    _df->add_float_column(col, containers::DataFrame::ROLE_NUMERICAL);
  }

  for (size_t i = 0; i < numerical.size(); ++i) {
    auto col = containers::Column<Float>(_numerical_features.at(j++).ptr())
                   .clone(_df->pool());
    col.set_name(numerical.at(i));
    _df->add_float_column(col, containers::DataFrame::ROLE_NUMERICAL);
  }

  assert_true(categorical.size() == _categorical_features.size());

  for (size_t i = 0; i < categorical.size(); ++i) {
    auto col = containers::Column<Int>(_categorical_features.at(i).ptr())
                   .clone(_df->pool());
    col.set_name(categorical.at(i));
    _df->add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::add_join_keys_to_df(
    const containers::DataFrame& _population_table,
    containers::DataFrame* _df) const {
  for (size_t i = 0; i < _population_table.num_join_keys(); ++i) {
    auto col = _population_table.join_key(i).clone(_df->pool());

    if (col.name().find(helpers::Macros::multiple_join_key_begin()) !=
        std::string::npos) {
      continue;
    }

    if (col.name().find(helpers::Macros::no_join_key()) != std::string::npos) {
      continue;
    }

    const auto make_staging_table_colname =
        [](const std::string& _colname) -> std::string {
      return transpilation::HumanReadableSQLGenerator()
          .make_staging_table_colname(_colname);
    };

    col.set_name(helpers::Macros::modify_colnames({col.name()},
                                                  make_staging_table_colname)
                     .at(0));

    _df->add_int_column(col, containers::DataFrame::ROLE_JOIN_KEY);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::add_predictions_to_df(
    const pipelines::FittedPipeline& _fitted,
    const containers::NumericalFeatures& _numerical_features,
    containers::DataFrame* _df) const {
  const auto& targets = _fitted.targets();

  assert_true(targets.size() == _numerical_features.size());

  for (size_t i = 0; i < targets.size(); ++i) {
    auto col = containers::Column<Float>(_numerical_features.at(i).ptr());
    col.set_name("prediction_" + std::to_string(i + 1) + "__" + targets.at(i));
    _df->add_float_column(col, containers::DataFrame::ROLE_NUMERICAL);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::add_time_stamps_to_df(
    const containers::DataFrame& _population_table,
    containers::DataFrame* _df) const {
  for (size_t i = 0; i < _population_table.num_time_stamps(); ++i) {
    auto col = _population_table.time_stamp(i).clone(_df->pool());

    if (col.name().find(helpers::Macros::lower_ts()) != std::string::npos) {
      continue;
    }

    if (col.name().find(helpers::Macros::other_time_stamp()) !=
        std::string::npos) {
      continue;
    }

    if (col.name().find(helpers::Macros::rowid()) != std::string::npos) {
      continue;
    }

    if (col.name().find(helpers::Macros::upper_time_stamp()) !=
        std::string::npos) {
      continue;
    }

    if (col.name().find(helpers::Macros::upper_ts()) != std::string::npos) {
      continue;
    }

    const auto make_staging_table_colname =
        [](const std::string& _colname) -> std::string {
      return transpilation::HumanReadableSQLGenerator()
          .make_staging_table_colname(_colname);
    };

    col.set_name(helpers::Macros::modify_colnames({col.name()},
                                                  make_staging_table_colname)
                     .at(0));

    _df->add_float_column(col, containers::DataFrame::ROLE_TIME_STAMP);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::add_to_tracker(
    const pipelines::FittedPipeline& _fitted,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    containers::DataFrame* _df) {
  const auto dependencies = _fitted.fingerprints_.get<"fs_fingerprints_">();

  const auto build_history = data_frame_tracker().make_build_history(
      *dependencies, _population_df, _peripheral_dfs);

  _df->set_build_history(build_history);

  data_frame_tracker().add(*_df, build_history);
}

// ------------------------------------------------------------------------

void PipelineManager::check(const typename Command::CheckOp& _cmd,
                            Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto [pipeline, was_loaded] = load_if_necessary(name);

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      fct::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const commands::DataFramesOrViews cmd = _cmd;

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding,
                 params_.data_frames_, params_.options_)
          .parse_all(cmd);

  const auto params = pipelines::CheckParams(
      fct::make_field<"categories_">(local_categories),
      fct::make_field<"cmd_", commands::DataFramesOrViews>(_cmd),
      fct::make_field<"logger_">(params_.logger_.ptr()),
      fct::make_field<"peripheral_dfs_">(peripheral_dfs),
      fct::make_field<"population_df_">(population_df),
      fct::make_field<"preprocessor_tracker_">(params_.preprocessor_tracker_),
      fct::make_field<"warning_tracker_">(params_.warning_tracker_),
      fct::make_field<"socket_">(_socket));

  pipelines::check::check(pipeline, params);

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  weak_write_lock.unlock();

  if (was_loaded) {
    set_pipeline(name, pipeline);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::check_user_privileges(
    const pipelines::Pipeline& _pipeline, const std::string& _name,
    const fct::NamedTuple<fct::Field<"http_request_", bool>>& _cmd) const {
  if (_cmd.get<"http_request_">()) {
    if (!_pipeline.allow_http()) {
      throw std::runtime_error(
          "Pipeline '" + _name +
          "' does not allow HTTP requests. You can activate "
          "this "
          "via the API or the getML monitor!");
    }
  }
}

// ------------------------------------------------------------------------

void PipelineManager::column_importances(
    const typename Command::ColumnImportancesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto target_num = _cmd.get<"target_num_">();

  const auto pipeline = get_pipeline(name);

  const auto scores = pipeline.scores();

  auto importances = std::vector<Float>();

  for (const auto& vec : scores.column_importances()) {
    if (target_num < 0) {
      const auto sum_importances = std::accumulate(vec.begin(), vec.end(), 0.0);

      const auto length = static_cast<Float>(vec.size());

      importances.push_back(sum_importances / length);

      continue;
    }

    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::runtime_error("target_num out of range!");
    }

    importances.push_back(vec.at(target_num));
  }

  const auto response =
      fct::make_field<"column_descriptions_">(scores.column_descriptions()) *
      fct::make_field<"column_importances_">(importances);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::deploy(const typename Command::DeployOp& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const bool deploy = _cmd.get<"deploy_">();

  const auto pipeline = get_pipeline(name).with_allow_http(deploy);

  set_pipeline(name, pipeline);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_correlations(
    const typename Command::FeatureCorrelationsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto target_num = _cmd.get<"target_num_">();

  const auto pipeline = get_pipeline(name);

  const auto scores = pipeline.scores();

  auto correlations = std::vector<Float>();

  for (const auto& vec : scores.feature_correlations()) {
    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::runtime_error("target_num out of range!");
    }
    correlations.push_back(vec.at(target_num));
  }

  const auto response =
      fct::make_field<"feature_names_">(scores.feature_names()) *
      fct::make_field<"feature_correlations_">(correlations);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_importances(
    const typename Command::FeatureImportancesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto target_num = _cmd.get<"target_num_">();

  const auto pipeline = get_pipeline(name);

  const auto scores = pipeline.scores();

  auto importances = std::vector<Float>();

  for (const auto& vec : scores.feature_importances()) {
    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::runtime_error("target_num out of range!");
    }

    importances.push_back(vec.at(target_num));
  }

  const auto response =
      fct::make_field<"feature_names_">(scores.feature_names()) *
      fct::make_field<"feature_importances_">(importances);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::fit(const typename Command::FitOp& _cmd,
                          Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  auto pipeline = get_pipeline(name);

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      fct::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto [population_df, peripheral_dfs, validation_df] =
      ViewParser(local_categories, local_join_keys_encoding,
                 params_.data_frames_, params_.options_)
          .parse_all(_cmd);

  const auto params = pipelines::FitParams(
      fct::make_field<"categories_">(local_categories),
      fct::make_field<"cmd_", commands::DataFramesOrViews>(_cmd),
      fct::make_field<"data_frames_">(data_frames()),
      fct::make_field<"data_frame_tracker_">(data_frame_tracker()),
      fct::make_field<"fe_tracker_">(params_.fe_tracker_),
      fct::make_field<"fs_fingerprints_">(
          fct::Ref<const std::vector<commands::Fingerprint>>::make()),
      fct::make_field<"logger_">(params_.logger_.ptr()),
      fct::make_field<"peripheral_dfs_">(peripheral_dfs),
      fct::make_field<"population_df_">(population_df),
      fct::make_field<"pred_tracker_">(params_.pred_tracker_),
      fct::make_field<"preprocessor_tracker_">(params_.preprocessor_tracker_),
      fct::make_field<"validation_df_">(validation_df),
      fct::make_field<"socket_">(_socket));

  const auto [fitted, scores] = pipelines::fit::fit(pipeline, params);

  pipeline = pipeline.with_fitted(fitted).with_scores(scores);

  const auto it = pipelines().find(name);

  if (it == pipelines().end()) {
    throw std::runtime_error("Pipeline '" + name + "' does not exist!");
  }

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  it->second = pipeline;

  weak_write_lock.unlock();

  communication::Sender::send_string("Trained pipeline.", _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::lift_curve(const typename Command::LiftCurveOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto target_num = _cmd.get<"target_num_">();

  const auto pipeline = get_pipeline(name);

  const auto& scores = pipeline.scores();

  const auto result =
      fct::make_field<"proportion_">(scores.proportion(target_num)) *
      fct::make_field<"lift_">(scores.lift(target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(result), _socket);
}

// ------------------------------------------------------------------------

std::pair<pipelines::Pipeline, bool> PipelineManager::load_if_necessary(
    const std::string& _name) {
  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& p = utils::Getter::get(_name, &pipelines());

  if (p.fitted()) {
    return std::make_pair(p, false);
  }

  const auto path =
      params_.options_.project_directory() + "pipelines/" + _name + "/";

  if (!std::filesystem::exists(path)) {
    return std::make_pair(p, false);
  }

  const auto pipeline_trackers = dependency::PipelineTrackers(
      fct::make_field<"data_frame_tracker_">(params_.data_frame_tracker_),
      fct::make_field<"fe_tracker_">(params_.fe_tracker_),
      fct::make_field<"pred_tracker_">(params_.pred_tracker_),
      fct::make_field<"preprocessor_tracker_">(params_.preprocessor_tracker_));

  const auto pipeline_with_fitted =
      pipelines::load_fitted::load_fitted(path, p, pipeline_trackers);

  return std::make_pair(std::move(pipeline_with_fitted), true);
}

// ------------------------------------------------------------------------

void PipelineManager::precision_recall_curve(
    const typename Command::PrecisionRecallCurveOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto target_num = _cmd.get<"target_num_">();

  const auto pipeline = get_pipeline(name);

  const auto& scores = pipeline.scores();

  const auto result =
      fct::make_field<"precision_">(scores.precision(target_num)) *
      fct::make_field<"tpr_">(scores.tpr(target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(result), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::refresh(const typename Command::RefreshOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto [pipeline, was_loaded] = load_if_necessary(name);

  if (!pipeline.fitted()) {
    throw std::runtime_error("Pipeline '" + name +
                             "' cannot be refreshed. It has not been fitted.");
  }

  const auto obj = refresh_pipeline(pipeline);

  communication::Sender::send_string(json::to_json(obj), _socket);

  if (was_loaded) {
    set_pipeline(name, pipeline);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::refresh_all(const typename Command::RefreshAllOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  using namespace std::ranges::views;

  std::map<std::string, pipelines::Pipeline> updated_pipelines;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto names = pipelines() | keys;

  auto vec = std::vector<RefreshPipelineType>();

  for (const auto name : names) {
    const auto [pipe, was_loaded] = load_if_necessary(name);
    if (!pipe.fitted()) {
      continue;
    }
    vec.push_back(refresh_pipeline(pipe));
    if (was_loaded) {
      updated_pipelines.insert(std::make_pair(name, pipe));
    }
  }

  communication::Sender::send_string("Success!", _socket);

  const auto obj = fct::make_named_tuple(fct::make_field<"pipelines">(vec));

  communication::Sender::send_string(json::to_json(obj), _socket);

  read_lock.unlock();

  for (const auto& [name, pipe] : updated_pipelines) {
    set_pipeline(name, pipe);
  }
}

// ------------------------------------------------------------------------

typename PipelineManager::RefreshPipelineType PipelineManager::refresh_pipeline(
    const pipelines::Pipeline& _pipeline) const {
  const auto extract_roles = [](const helpers::Schema& _schema) -> RolesType {
    return fct::make_field<"name">(_schema.name()) *
           fct::make_field<"roles">(containers::Roles::from_schema(_schema));
  };

  const ScoresType scores =
      _pipeline.scores().metrics() *
      fct::make_field<"set_used_">(_pipeline.scores().set_used()) *
      fct::make_field<"history_">(_pipeline.scores().history());

  const RefreshUnfittedPipelineType base =
      fct::make_field<"obj">(_pipeline.obj()) *
      fct::make_field<"scores">(scores);

  if (!_pipeline.fitted()) {
    return base;
  }

  const auto peripheral_metadata =
      fct::collect::vector(*_pipeline.fitted()->peripheral_schema_ |
                           VIEWS::transform(extract_roles));

  const auto population_metadata =
      extract_roles(*_pipeline.fitted()->population_schema_);

  const auto& targets = _pipeline.fitted()->targets();

  return RefreshFittedPipelineType(
      base * fct::make_field<"peripheral_metadata">(peripheral_metadata) *
      fct::make_field<"population_metadata">(population_metadata) *
      fct::make_field<"targets">(targets));
}

// ------------------------------------------------------------------------

void PipelineManager::roc_curve(const typename Command::ROCCurveOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto target_num = _cmd.get<"target_num_">();

  const auto pipeline = get_pipeline(name);

  const auto& scores = pipeline.scores();

  const auto result = fct::make_field<"fpr_">(scores.fpr(target_num)) *
                      fct::make_field<"tpr_">(scores.tpr(target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(result), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::score(const FullTransformOp& _cmd,
                            const std::string& _name,
                            const containers::DataFrame& _population_df,
                            const containers::NumericalFeatures& _yhat,
                            const pipelines::Pipeline& _pipeline,
                            Poco::Net::StreamSocket* _socket) {
  const auto population_json = _cmd.get<"population_df_">();

  const auto name = fct::get<"name_">(population_json.val_);

  const auto fitted = _pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error(
        "Could not score the pipeline, because it has not been fitted.");
  }

  const auto scores =
      pipelines::score::score(_pipeline, *fitted, _population_df, name, _yhat);

  const auto pipeline = _pipeline.with_scores(scores);

  communication::Sender::send_string("Success!", _socket);

  set_pipeline(_name, pipeline);

  communication::Sender::send_string(json::to_json(scores->metrics()), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::store_df(
    const pipelines::FittedPipeline& _fitted, const FullTransformOp& _cmd,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const fct::Ref<containers::Encoding>& _local_categories,
    const fct::Ref<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock) {
  _weak_write_lock->upgrade();

  params_.categories_->append(*_local_categories);

  params_.join_keys_encoding_->append(*_local_join_keys_encoding);

  _df->set_categories(params_.categories_.ptr());  // TODO

  _df->set_join_keys_encoding(params_.join_keys_encoding_.ptr());  // TODO

  const auto predict = _cmd.get<"predict_">();

  if (!predict) {
    add_to_tracker(_fitted, _population_df, _peripheral_dfs, _df);
  }

  data_frames()[_df->name()] = *_df;
}

// ------------------------------------------------------------------------

void PipelineManager::to_db(
    const pipelines::FittedPipeline& _fitted, const FullTransformOp& _cmd,
    const containers::DataFrame& _population_table,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    const fct::Ref<containers::Encoding>& _categories,
    const fct::Ref<containers::Encoding>& _join_keys_encoding) {
  const auto df =
      to_df(_fitted, _cmd, _population_table, _numerical_features,
            _categorical_features, _categories, _join_keys_encoding);

  const auto table_name = _cmd.get<"table_name_">();

  // We are using the bell character (\a) as the quotechar. It is least likely
  // to appear in any field.
  auto reader = containers::DataFrameReader(
      df, _categories.ptr(), _join_keys_encoding.ptr(), '\a', '|');

  const auto conn = connector("default");

  const auto statement = io::StatementMaker::make_statement(
      table_name, conn->dialect(), reader.colnames(), reader.coltypes());

  logger().log(statement);

  conn->execute(statement);

  conn->read(table_name, 0, &reader);

  params_.database_manager_->post_tables();
}

// ------------------------------------------------------------------------

containers::DataFrame PipelineManager::to_df(
    const pipelines::FittedPipeline& _fitted, const FullTransformOp& _cmd,
    const containers::DataFrame& _population_table,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    const fct::Ref<containers::Encoding>& _categories,
    const fct::Ref<containers::Encoding>& _join_keys_encoding) {
  const auto df_name = _cmd.get<"df_name_">();

  const auto pool = params_.options_.make_pool();

  auto df = containers::DataFrame(df_name, _categories.ptr(),
                                  _join_keys_encoding.ptr(), pool);  // TODO

  if (!_cmd.get<"predict_">()) {
    add_features_to_df(_fitted, _numerical_features, _categorical_features,
                       &df);
  } else {
    add_predictions_to_df(_fitted, _numerical_features, &df);
  }

  add_join_keys_to_df(_population_table, &df);

  add_time_stamps_to_df(_population_table, &df);

  for (size_t i = 0; i < _population_table.num_targets(); ++i) {
    const auto col = _population_table.target(i).clone(df.pool());
    df.add_float_column(col, containers::DataFrame::ROLE_TARGET);
  }

  return df;
}

// ------------------------------------------------------------------------

void PipelineManager::to_sql(const typename Command::ToSQLOp& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto name = _cmd.get<"name_">();

  const auto targets = _cmd.get<"targets_">();

  const auto subfeatures = _cmd.get<"subfeatures_">();

  const auto size_threshold = _cmd.get<"size_threshold_">();

  const auto transpilation_params = transpilation::TranspilationParams(_cmd);

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto [pipeline, was_loaded] = load_if_necessary(name);

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error(
        "Could not transpile the pipeline to SQL, because it has not been "
        "fitted.");
  }

  const auto params =
      pipelines::ToSQLParams{.categories_ = categories().strings(),
                             .fitted_ = *fitted,
                             .full_pipeline_ = subfeatures,
                             .pipeline_ = pipeline,
                             .size_threshold_ = size_threshold,
                             .targets_ = targets,
                             .transpilation_params_ = transpilation_params};

  const auto sql = pipelines::to_sql::to_sql(params);

  read_lock.unlock();

  if (was_loaded) {
    set_pipeline(name, pipeline);
  }

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(sql, _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::transform(const typename Command::TransformOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  auto [pipeline, was_loaded] = load_if_necessary(name);

  check_user_privileges(pipeline, name, _cmd);

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      fct::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  auto local_data_frames =
      fct::Ref<std::map<std::string, containers::DataFrame>>::make(
          data_frames());

  const auto cmd =
      receive_data(_cmd, local_categories, local_join_keys_encoding,
                   local_data_frames, _socket);

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding, local_data_frames,
                 params_.options_)
          .parse_all(cmd);

  // IMPORTANT: Use categories_, not local_categories, otherwise
  // .vector() might not work.
  const auto params = pipelines::TransformParams(
      fct::make_field<"categories_">(params_.categories_),
      fct::make_field<"cmd_", pipelines::TransformCmdType>(cmd),
      fct::make_field<"data_frames_">(*local_data_frames),
      fct::make_field<"data_frame_tracker_">(data_frame_tracker()),
      fct::make_field<"logger_">(params_.logger_.ptr()),
      fct::make_field<"original_peripheral_dfs_">(peripheral_dfs),
      fct::make_field<"original_population_df_">(population_df),
      fct::make_field<"socket_">(_socket));

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error("The pipeline has not been fitted.");
  }

  const auto [numerical_features, categorical_features, scores] =
      pipelines::transform::transform(params, pipeline, *fitted);

  if (scores) {
    pipeline = pipeline.with_scores(fct::Ref<const metrics::Scores>(scores));
  }

  const auto& table_name = cmd.get<"table_name_">();

  const auto& df_name = cmd.get<"df_name_">();

  const bool scoring_required = cmd.get<"score_">();

  if (table_name == "" && df_name == "" && !scoring_required) {
    communication::Sender::send_string("Success!", _socket);
    communication::Sender::send_features(numerical_features, _socket);
    return;
  }

  if (table_name != "") {
    to_db(*fitted, cmd, population_df, numerical_features, categorical_features,
          local_categories, local_join_keys_encoding);
  }

  if (df_name != "") {
    auto df =
        to_df(*fitted, cmd, population_df, numerical_features,
              categorical_features, local_categories, local_join_keys_encoding);

    store_df(*fitted, cmd, population_df, peripheral_dfs, local_categories,
             local_join_keys_encoding, &df, &weak_write_lock);
  }

  communication::Sender::send_string("Success!", _socket);

  weak_write_lock.unlock();

  if (scoring_required) {
    score(cmd, name, population_df, numerical_features, pipeline, _socket);
  } else if (was_loaded) {
    set_pipeline(name, pipeline);
  }
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
