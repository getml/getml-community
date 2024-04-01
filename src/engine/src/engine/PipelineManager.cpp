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
#include "rfl/Field.hpp"
#include "rfl/always_false.hpp"
#include "rfl/as.hpp"
#include "rfl/make_named_tuple.hpp"
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
  const auto dependencies = _fitted.fingerprints_.fs_fingerprints();

  const auto build_history = data_frame_tracker().make_build_history(
      *dependencies, _population_df, _peripheral_dfs);

  _df->set_build_history(build_history);

  data_frame_tracker().add(*_df, build_history);
}

// ------------------------------------------------------------------------

void PipelineManager::check(const typename Command::CheckOp& _cmd,
                            Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto pipeline = utils::Getter::get(name, pipelines());

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto cmd = rfl::as<commands::DataFramesOrViews>(_cmd);

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding,
                 params_.data_frames_, params_.options_)
          .parse_all(cmd);

  const auto params = pipelines::CheckParams(
      rfl::make_field<"categories_">(local_categories),
      rfl::make_field<"cmd_">(cmd),
      rfl::make_field<"logger_">(params_.logger_.ptr()),
      rfl::make_field<"peripheral_dfs_">(peripheral_dfs),
      rfl::make_field<"population_df_">(population_df),
      rfl::make_field<"preprocessor_tracker_">(params_.preprocessor_tracker_),
      rfl::make_field<"warning_tracker_">(params_.warning_tracker_),
      rfl::make_field<"socket_">(_socket));

  pipelines::check::check(pipeline, params);

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  weak_write_lock.unlock();
}

// ------------------------------------------------------------------------

void PipelineManager::check_user_privileges(
    const pipelines::Pipeline& _pipeline, const std::string& _name,
    const rfl::NamedTuple<rfl::Field<"http_request_", bool>>& _cmd) const {
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
  const auto& name = _cmd.name();

  const auto target_num = _cmd.target_num();

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
      rfl::make_field<"column_descriptions_">(scores.column_descriptions()) *
      rfl::make_field<"column_importances_">(importances);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::deploy(const typename Command::DeployOp& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const bool deploy = _cmd.deploy();

  const auto pipeline = get_pipeline(name).with_allow_http(deploy);

  set_pipeline(name, pipeline);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_correlations(
    const typename Command::FeatureCorrelationsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto target_num = _cmd.target_num();

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
      rfl::make_field<"feature_names_">(scores.feature_names()) *
      rfl::make_field<"feature_correlations_">(correlations);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_importances(
    const typename Command::FeatureImportancesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto target_num = _cmd.target_num();

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
      rfl::make_field<"feature_names_">(scores.feature_names()) *
      rfl::make_field<"feature_importances_">(importances);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::fit(const typename Command::FitOp& _cmd,
                          Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  auto pipeline = get_pipeline(name);

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto cmd = rfl::as<commands::DataFramesOrViews>(_cmd);

  const auto [population_df, peripheral_dfs, validation_df] =
      ViewParser(local_categories, local_join_keys_encoding,
                 params_.data_frames_, params_.options_)
          .parse_all(cmd);

  const auto params = pipelines::FitParams(
      rfl::make_field<"categories_">(local_categories),
      rfl::make_field<"cmd_">(cmd),
      rfl::make_field<"data_frames_">(data_frames()),
      rfl::make_field<"data_frame_tracker_">(data_frame_tracker()),
      rfl::make_field<"fe_tracker_">(params_.fe_tracker_),
      rfl::make_field<"fs_fingerprints_">(
          rfl::Ref<const std::vector<commands::Fingerprint>>::make()),
      rfl::make_field<"logger_">(params_.logger_.ptr()),
      rfl::make_field<"peripheral_dfs_">(peripheral_dfs),
      rfl::make_field<"population_df_">(population_df),
      rfl::make_field<"pred_tracker_">(params_.pred_tracker_),
      rfl::make_field<"preprocessor_tracker_">(params_.preprocessor_tracker_),
      rfl::make_field<"validation_df_">(validation_df),
      rfl::make_field<"socket_">(_socket));

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
  const auto& name = _cmd.name();

  const auto target_num = _cmd.target_num();

  const auto pipeline = get_pipeline(name);

  const auto& scores = pipeline.scores();

  const auto result =
      rfl::make_field<"proportion_">(scores.proportion(target_num)) *
      rfl::make_field<"lift_">(scores.lift(target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(result), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::precision_recall_curve(
    const typename Command::PrecisionRecallCurveOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto target_num = _cmd.target_num();

  const auto pipeline = get_pipeline(name);

  const auto& scores = pipeline.scores();

  const auto result =
      rfl::make_field<"precision_">(scores.precision(target_num)) *
      rfl::make_field<"tpr_">(scores.tpr(target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(result), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::refresh(const typename Command::RefreshOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto pipeline = utils::Getter::get(name, pipelines());

  if (!pipeline.fitted()) {
    throw std::runtime_error("Pipeline '" + name +
                             "' cannot be refreshed. It has not been fitted.");
  }

  const auto obj = refresh_pipeline(pipeline);

  communication::Sender::send_string(rfl::json::write(obj), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::refresh_all(const typename Command::RefreshAllOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  using namespace std::ranges::views;

  std::map<std::string, pipelines::Pipeline> updated_pipelines;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto names = pipelines() | keys;

  auto vec = std::vector<RefreshPipelineType>();

  for (const auto& name : names) {
    const auto pipe = utils::Getter::get(name, pipelines());
    if (!pipe.fitted()) {
      continue;
    }
    vec.push_back(refresh_pipeline(pipe));
  }

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  const auto obj = rfl::make_named_tuple(rfl::make_field<"pipelines">(vec));

  communication::Sender::send_string(rfl::json::write(obj), _socket);

  for (const auto& [name, pipe] : updated_pipelines) {
    set_pipeline(name, pipe);
  }
}

// ------------------------------------------------------------------------

typename PipelineManager::RefreshPipelineType PipelineManager::refresh_pipeline(
    const pipelines::Pipeline& _pipeline) const {
  const auto extract_roles = [](const helpers::Schema& _schema) -> RolesType {
    return rfl::make_field<"name">(_schema.name()) *
           rfl::make_field<"roles">(containers::Roles::from_schema(_schema));
  };

  const ScoresType scores =
      _pipeline.scores().metrics() *
      rfl::make_field<"set_used_">(_pipeline.scores().set_used()) *
      rfl::make_field<"history_">(_pipeline.scores().history());

  const RefreshUnfittedPipelineType base =
      rfl::make_field<"obj">(_pipeline.obj()) *
      rfl::make_field<"scores">(scores);

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
      base * rfl::make_field<"peripheral_metadata">(peripheral_metadata) *
      rfl::make_field<"population_metadata">(population_metadata) *
      rfl::make_field<"targets">(targets));
}

// ------------------------------------------------------------------------

void PipelineManager::roc_curve(const typename Command::ROCCurveOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto target_num = _cmd.target_num();

  const auto pipeline = get_pipeline(name);

  const auto& scores = pipeline.scores();

  const auto result = rfl::make_field<"fpr_">(scores.fpr(target_num)) *
                      rfl::make_field<"tpr_">(scores.tpr(target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(result), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::score(const FullTransformOp& _cmd,
                            const std::string& _name,
                            const containers::DataFrame& _population_df,
                            const containers::NumericalFeatures& _yhat,
                            const pipelines::Pipeline& _pipeline,
                            Poco::Net::StreamSocket* _socket) {
  const auto population_df = _cmd.population_df();

  const auto get_name = [](const auto& _c) { return _c.name(); };

  const auto name = rfl::visit(get_name, population_df.val_);

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

  communication::Sender::send_string(rfl::json::write(scores->metrics()),
                                     _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::store_df(
    const pipelines::FittedPipeline& _fitted, const FullTransformOp& _cmd,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const rfl::Ref<containers::Encoding>& _local_categories,
    const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock) {
  _weak_write_lock->upgrade();

  params_.categories_->append(*_local_categories);

  params_.join_keys_encoding_->append(*_local_join_keys_encoding);

  _df->set_categories(params_.categories_.ptr());  // TODO

  _df->set_join_keys_encoding(params_.join_keys_encoding_.ptr());  // TODO

  const auto predict = _cmd.predict();

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
    const rfl::Ref<containers::Encoding>& _categories,
    const rfl::Ref<containers::Encoding>& _join_keys_encoding) {
  const auto df =
      to_df(_fitted, _cmd, _population_table, _numerical_features,
            _categorical_features, _categories, _join_keys_encoding);

  const auto table_name = _cmd.table_name();

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
    const rfl::Ref<containers::Encoding>& _categories,
    const rfl::Ref<containers::Encoding>& _join_keys_encoding) {
  const auto df_name = _cmd.df_name();

  const auto pool = params_.options_.make_pool();

  auto df = containers::DataFrame(df_name, _categories.ptr(),
                                  _join_keys_encoding.ptr(), pool);  // TODO

  if (!_cmd.predict()) {
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
  const auto name = _cmd.name();

  const auto targets = _cmd.targets();

  const auto subfeatures = _cmd.subfeatures();

  const auto size_threshold = _cmd.size_threshold();

  const auto transpilation_params = _cmd.transpilation_params();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto pipeline = utils::Getter::get(name, pipelines());

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error(
        "Could not transpile the pipeline to SQL, because it has not been "
        "fitted.");
  }

  const auto params =
      pipelines::ToSQLParams{.categories = categories().strings(),
                             .fitted = *fitted,
                             .full_pipeline = subfeatures,
                             .pipeline = pipeline,
                             .size_threshold = size_threshold,
                             .targets = targets,
                             .transpilation_params = transpilation_params};

  const auto sql = pipelines::to_sql::to_sql(params);

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(sql, _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::transform(const typename Command::TransformOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  auto pipeline = utils::Getter::get(name, pipelines());

  check_user_privileges(pipeline, name, rfl::to_named_tuple(_cmd));

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  auto local_data_frames =
      rfl::Ref<std::map<std::string, containers::DataFrame>>::make(
          data_frames());

  const auto cmd =
      receive_data(_cmd, local_categories, local_join_keys_encoding,
                   local_data_frames, _socket);

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding, local_data_frames,
                 params_.options_)
          .parse_all(rfl::as<commands::DataFramesOrViews>(cmd));

  // IMPORTANT: Use categories_, not local_categories, otherwise
  // .vector() might not work.
  const auto params = pipelines::TransformParams{
      .categories = params_.categories_,
      .cmd = rfl::as<pipelines::TransformParams::Cmd>(cmd),
      .data_frames = *local_data_frames,
      .data_frame_tracker = data_frame_tracker(),
      .logger = params_.logger_.ptr(),
      .original_peripheral_dfs = peripheral_dfs,
      .original_population_df = population_df,
      .socket = _socket};

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error("The pipeline has not been fitted.");
  }

  const auto [numerical_features, categorical_features, scores] =
      pipelines::transform::transform(params, pipeline, *fitted);

  if (scores) {
    pipeline =
        pipeline.with_scores(*rfl::Ref<const metrics::Scores>::make(scores));
  }

  const auto& table_name = cmd.table_name();

  const auto& df_name = cmd.df_name();

  const bool scoring_required = cmd.score();

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

  weak_write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  if (scoring_required) {
    score(cmd, name, population_df, numerical_features, pipeline, _socket);
  }
}

}  // namespace handlers
}  // namespace engine
