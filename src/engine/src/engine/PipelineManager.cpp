// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/PipelineManager.hpp"

#include <stdexcept>

#include "engine/containers/Roles.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/pipelines/ToSQL.hpp"
#include "engine/pipelines/ToSQLParams.hpp"
#include "engine/pipelines/pipelines.hpp"
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
  const auto dependencies = _fitted.fingerprints_.fs_fingerprints_;

  const auto build_history = data_frame_tracker().make_build_history(
      dependencies, _population_df, _peripheral_dfs);

  _df->set_build_history(build_history);

  data_frame_tracker().add(*_df);
}

// ------------------------------------------------------------------------

void PipelineManager::check(const std::string& _name,
                            const Poco::JSON::Object& _cmd,
                            Poco::Net::StreamSocket* _socket) {
  const auto pipeline = get_pipeline(_name);

  communication::Sender::send_string("Found!", _socket);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      fct::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding,
                 params_.data_frames_, params_.options_)
          .parse_all(_cmd);

  const auto params = pipelines::CheckParams{
      .categories_ = local_categories,
      .cmd_ = _cmd,
      .logger_ = params_.logger_.ptr(),
      .peripheral_dfs_ = peripheral_dfs,
      .population_df_ = population_df,
      .preprocessor_tracker_ = params_.preprocessor_tracker_,
      .warning_tracker_ = params_.warning_tracker_,
      .socket_ = _socket};

  pipelines::Check::check(pipeline, params);

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  weak_write_lock.unlock();
}

// ------------------------------------------------------------------------

void PipelineManager::check_user_privileges(
    const pipelines::Pipeline& _pipeline, const std::string& _name,
    const Poco::JSON::Object& _cmd) const {
  if (JSON::get_value<bool>(_cmd, "http_request_")) {
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

void PipelineManager::column_importances(const std::string& _name,
                                         const Poco::JSON::Object& _cmd,
                                         Poco::Net::StreamSocket* _socket) {
  const auto target_num = JSON::get_value<Int>(_cmd, "target_num_");

  const auto pipeline = get_pipeline(_name);

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

  Poco::JSON::Object response;

  response.set("column_descriptions_",
               JSON::vector_to_array(scores.column_descriptions()));

  response.set("column_importances_", JSON::vector_to_array(importances));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::deploy(const std::string& _name,
                             const Poco::JSON::Object& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const bool deploy = JSON::get_value<bool>(_cmd, "deploy_");

  const auto pipeline = get_pipeline(_name).with_allow_http(deploy);

  set_pipeline(_name, pipeline);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_correlations(const std::string& _name,
                                           const Poco::JSON::Object& _cmd,
                                           Poco::Net::StreamSocket* _socket) {
  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores();

  auto correlations = std::vector<Float>();

  for (const auto& vec : scores.feature_correlations()) {
    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::runtime_error("target_num out of range!");
    }

    correlations.push_back(vec.at(target_num));
  }

  Poco::JSON::Object response;

  response.set("feature_names_", JSON::vector_to_array(scores.feature_names()));

  response.set("feature_correlations_", JSON::vector_to_array(correlations));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_importances(const std::string& _name,
                                          const Poco::JSON::Object& _cmd,
                                          Poco::Net::StreamSocket* _socket) {
  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores();

  auto importances = std::vector<Float>();

  for (const auto& vec : scores.feature_importances()) {
    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::runtime_error("target_num out of range!");
    }

    importances.push_back(vec.at(target_num));
  }

  Poco::JSON::Object response;

  response.set("feature_names_", JSON::vector_to_array(scores.feature_names()));

  response.set("feature_importances_", JSON::vector_to_array(importances));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::fit(const std::string& _name,
                          const Poco::JSON::Object& _cmd,
                          Poco::Net::StreamSocket* _socket) {
  auto pipeline = get_pipeline(_name);

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

  const auto params = pipelines::FitParams{
      .categories_ = local_categories,
      .cmd_ = _cmd,
      .data_frames_ = data_frames(),
      .data_frame_tracker_ = data_frame_tracker(),
      .fe_tracker_ = params_.fe_tracker_,
      .logger_ = params_.logger_.ptr(),
      .peripheral_dfs_ = peripheral_dfs,
      .population_df_ = population_df,
      .pred_tracker_ = params_.pred_tracker_,
      .preprocessor_tracker_ = params_.preprocessor_tracker_,
      .validation_df_ = validation_df,
      .socket_ = _socket};

  const auto [fitted, scores] = pipelines::Fit::fit(pipeline, params);

  pipeline = pipeline.with_fitted(fitted).with_scores(scores);

  const auto it = pipelines().find(_name);

  if (it == pipelines().end()) {
    throw std::runtime_error("Pipeline '" + _name + "' does not exist!");
  }

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  it->second = pipeline;

  weak_write_lock.unlock();

  communication::Sender::send_string("Trained pipeline.", _socket);
}

// ------------------------------------------------------------------------

Poco::JSON::Array::Ptr PipelineManager::get_array(
    const Poco::JSON::Object& _scores, const std::string& _name,
    const unsigned int _target_num) const {
  const auto arr = JSON::get_array(_scores, _name);

  if (static_cast<size_t>(_target_num) >= arr->size()) {
    std::string msg = "target_num_ out of bounds! Got " +
                      std::to_string(_target_num) + ", but '" + _name +
                      "' has " + std::to_string(arr->size()) + " entries.";

    if (arr->size() == 0) {
      msg += " Did you maybe for get to call .score(...)?";
    }

    throw std::runtime_error(msg);
  }

  return arr->getArray(_target_num);
}

// ------------------------------------------------------------------------

Poco::JSON::Object PipelineManager::get_scores(
    const pipelines::Pipeline& _pipeline) const {
  const auto scores = _pipeline.scores();

  const auto obj = scores.to_json_obj();

  auto response = metrics::Scorer::get_metrics(obj);

  if (scores.set_used() != "") {
    response.set("set_used_", scores.set_used());
  }

  response.set("history_", JSON::vector_to_array_ptr(scores.history()));

  return response;
}

// ------------------------------------------------------------------------

void PipelineManager::lift_curve(const std::string& _name,
                                 const Poco::JSON::Object& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores().to_json_obj();

  Poco::JSON::Object response;

  response.set("proportion_", get_array(scores, "proportion_", target_num));

  response.set("lift_", get_array(scores, "lift_", target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::precision_recall_curve(const std::string& _name,
                                             const Poco::JSON::Object& _cmd,
                                             Poco::Net::StreamSocket* _socket) {
  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores().to_json_obj();

  Poco::JSON::Object response;

  response.set("precision_", get_array(scores, "precision_", target_num));

  response.set("tpr_", get_array(scores, "tpr_", target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);
}

// ------------------------------------------------------------------------

Poco::JSON::Object PipelineManager::receive_data(
    const Poco::JSON::Object& _cmd,
    const fct::Ref<containers::Encoding>& _categories,
    const fct::Ref<containers::Encoding>& _join_keys_encoding,
    const fct::Ref<std::map<std::string, containers::DataFrame>>& _data_frames,
    Poco::Net::StreamSocket* _socket) {
  // Declare local variables. The idea of the local variables
  // is to prevent the global variables from being affected
  // by local data frames.

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto local_read_write_lock =
      fct::Ref<multithreading::ReadWriteLock>::make();

  const auto data_frame_manager_params =
      DataFrameManagerParams{.categories_ = _categories,
                             .database_manager_ = params_.database_manager_,
                             .data_frames_ = _data_frames,
                             .join_keys_encoding_ = _join_keys_encoding,
                             .logger_ = params_.logger_,
                             .monitor_ = params_.monitor_,
                             .options_ = params_.options_,
                             .read_write_lock_ = local_read_write_lock};

  auto local_data_frame_manager = DataFrameManager(data_frame_manager_params);

  auto cmd = _cmd;

  while (true) {
    const auto name = JSON::get_value<std::string>(cmd, "name_");

    const auto type = JSON::get_value<std::string>(cmd, "type_");

    if (type == "DataFrame") {
      local_data_frame_manager.add_data_frame(name, _socket);
    } else if (type == "DataFrame.from_query") {
      local_data_frame_manager.from_query(name, cmd, false, _socket);
    } else if (type == "DataFrame.from_json") {
      local_data_frame_manager.from_json(name, cmd, false, _socket);
    } else if (type == "FloatColumn.set_unit") {
      local_data_frame_manager.set_unit(name, cmd, _socket);
    } else if (type == "StringColumn.set_unit") {
      local_data_frame_manager.set_unit_categorical(name, cmd, _socket);
    } else {
      break;
    }

    cmd = communication::Receiver::recv_cmd(params_.logger_, _socket);
  }

  return cmd;
}

// ------------------------------------------------------------------------

void PipelineManager::refresh(const std::string& _name,
                              Poco::Net::StreamSocket* _socket) {
  const auto pipeline = get_pipeline(_name);

  const auto obj = refresh_pipeline(pipeline);

  communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::refresh_all(Poco::Net::StreamSocket* _socket) {
  Poco::JSON::Object obj;

  Poco::JSON::Array pipelines_arr;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  for (const auto& [_, pipe] : pipelines()) {
    pipelines_arr.add(refresh_pipeline(pipe));
  }

  obj.set("pipelines", pipelines_arr);

  engine::communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

Poco::JSON::Object PipelineManager::refresh_pipeline(
    const pipelines::Pipeline& _pipeline) const {
  const auto extract_roles =
      [](const helpers::Schema& _schema) -> Poco::JSON::Object::Ptr {
    auto ptr = Poco::JSON::Object::Ptr(new Poco::JSON::Object());
    ptr->set("name", _schema.name_);
    ptr->set("roles", containers::Roles::from_schema(_schema).to_json_obj());
    return ptr;
  };

  Poco::JSON::Object obj;

  obj.set("obj", _pipeline.obj());

  obj.set("scores", get_scores(_pipeline));

  if (_pipeline.fitted()) {
    obj.set(
        "peripheral_metadata",
        JSON::vector_to_array_ptr(fct::collect::vector<Poco::JSON::Object::Ptr>(
            *_pipeline.fitted()->peripheral_schema_ |
            VIEWS::transform(extract_roles))));

    obj.set("population_metadata",
            extract_roles(*_pipeline.fitted()->population_schema_));

    obj.set("targets", JSON::vector_to_array(_pipeline.fitted()->targets()));
  }

  return obj;
}

// ------------------------------------------------------------------------

void PipelineManager::roc_curve(const std::string& _name,
                                const Poco::JSON::Object& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores().to_json_obj();

  Poco::JSON::Object response;

  response.set("fpr_", get_array(scores, "fpr_", target_num));

  response.set("tpr_", get_array(scores, "tpr_", target_num));

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::score(const Poco::JSON::Object& _cmd,
                            const std::string& _name,
                            const containers::DataFrame& _population_df,
                            const containers::NumericalFeatures& _yhat,
                            const pipelines::Pipeline& _pipeline,
                            Poco::Net::StreamSocket* _socket) {
  const auto population_json = *JSON::get_object(_cmd, "population_df_");

  const auto name = JSON::get_value<std::string>(population_json, "name_");

  const auto fitted = _pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error(
        "Could not score the pipeline, because it has not been fitted.");
  }

  const auto [scores, scores_obj] =
      pipelines::Score::score(_pipeline, *fitted, _population_df, name, _yhat);

  const auto pipeline = _pipeline.with_scores(scores);

  communication::Sender::send_string("Success!", _socket);

  set_pipeline(_name, pipeline);

  communication::Sender::send_string(JSON::stringify(scores_obj), _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::store_df(
    const pipelines::FittedPipeline& _fitted, const Poco::JSON::Object& _cmd,
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

  const auto predict = JSON::get_value<bool>(_cmd, "predict_");

  if (!predict) {
    add_to_tracker(_fitted, _population_df, _peripheral_dfs, _df);
  }

  data_frames()[_df->name()] = *_df;
}

// ------------------------------------------------------------------------

void PipelineManager::to_db(
    const pipelines::FittedPipeline& _fitted, const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_table,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    const fct::Ref<containers::Encoding>& _categories,
    const fct::Ref<containers::Encoding>& _join_keys_encoding) {
  const auto df =
      to_df(_fitted, _cmd, _population_table, _numerical_features,
            _categorical_features, _categories, _join_keys_encoding);

  const auto table_name = JSON::get_value<std::string>(_cmd, "table_name_");

  // We are using the bell character (\a) as the quotechar. It is least likely
  // to appear in any field.
  auto reader = containers::DataFrameReader(
      df, _categories.ptr(), _join_keys_encoding.ptr(), '\a', '|');

  const auto conn = connector("default");

  const auto statement = io::StatementMaker::make_statement(
      table_name, conn->dialect(), conn->describe(), reader.colnames(),
      reader.coltypes());

  logger().log(statement);

  conn->execute(statement);

  conn->read(table_name, 0, &reader);

  params_.database_manager_->post_tables();
}

// ------------------------------------------------------------------------

containers::DataFrame PipelineManager::to_df(
    const pipelines::FittedPipeline& _fitted, const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_table,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    const fct::Ref<containers::Encoding>& _categories,
    const fct::Ref<containers::Encoding>& _join_keys_encoding) {
  const auto df_name = JSON::get_value<std::string>(_cmd, "df_name_");

  const auto pool = params_.options_.make_pool();

  auto df = containers::DataFrame(df_name, _categories.ptr(),
                                  _join_keys_encoding.ptr(), pool);  // TODO

  if (!_cmd.has("predict_") || !JSON::get_value<bool>(_cmd, "predict_")) {
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

void PipelineManager::to_sql(const std::string& _name,
                             const Poco::JSON::Object& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto targets = JSON::get_value<bool>(_cmd, "targets_");

  const auto subfeatures = JSON::get_value<bool>(_cmd, "subfeatures_");

  const auto size_threshold =
      _cmd.has("size_threshold_")
          ? JSON::get_value<size_t>(_cmd, "size_threshold_")
          : std::optional<size_t>();

  const auto transpilation_params =
      transpilation::TranspilationParams::from_json(_cmd);

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto pipeline = get_pipeline(_name);

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

  const auto sql = pipelines::ToSQL::to_sql(params);

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(sql, _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::transform(const std::string& _name,
                                const Poco::JSON::Object& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  auto pipeline = get_pipeline(_name);

  check_user_privileges(pipeline, _name, _cmd);

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

  auto cmd = communication::Receiver::recv_cmd(params_.logger_, _socket);

  cmd = receive_data(cmd, local_categories, local_join_keys_encoding,
                     local_data_frames, _socket);

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding, local_data_frames,
                 params_.options_)
          .parse_all(cmd);

  // IMPORTANT: Use categories_, not local_categories, otherwise
  // .vector() might not work.
  const auto params =
      pipelines::TransformParams{.categories_ = params_.categories_,
                                 .cmd_ = cmd,
                                 .data_frames_ = *local_data_frames,
                                 .data_frame_tracker_ = data_frame_tracker(),
                                 .logger_ = params_.logger_.ptr(),
                                 .original_peripheral_dfs_ = peripheral_dfs,
                                 .original_population_df_ = population_df,
                                 .socket_ = _socket};

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error("The pipeline has not been fitted.");
  }

  const auto [numerical_features, categorical_features, scores] =
      pipelines::Transform::transform(params, pipeline, *fitted);

  if (scores) {
    pipeline = pipeline.with_scores(fct::Ref<const metrics::Scores>(scores));
  }

  const auto table_name = JSON::get_value<std::string>(cmd, "table_name_");

  const auto df_name = JSON::get_value<std::string>(cmd, "df_name_");

  const auto score = JSON::get_value<bool>(cmd, "score_");

  if (table_name == "" && df_name == "" && !score) {
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

  if (score) {
    this->score(cmd, _name, population_df, numerical_features, pipeline,
                _socket);
  }
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
