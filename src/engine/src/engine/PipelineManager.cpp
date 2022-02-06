#include "engine/handlers/PipelineManager.hpp"

// ------------------------------------------------------------------------

#include "transpilation/transpilation.hpp"

// ------------------------------------------------------------------------

#include "engine/handlers/DataFrameManager.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace handlers {

void PipelineManager::add_features_to_df(
    const pipelines::Pipeline& _pipeline,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    containers::DataFrame* _df) const {
  const auto [autofeatures, numerical, categorical] = _pipeline.feature_names();

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
      return transpilation::SQLite3Generator().make_staging_table_colname(
          _colname);
    };

    col.set_name(helpers::Macros::modify_colnames({col.name()},
                                                  make_staging_table_colname)
                     .at(0));

    _df->add_int_column(col, containers::DataFrame::ROLE_JOIN_KEY);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::add_predictions_to_df(
    const pipelines::Pipeline& _pipeline,
    const containers::NumericalFeatures& _numerical_features,
    containers::DataFrame* _df) const {
  const auto targets = _pipeline.targets();

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
      return transpilation::SQLite3Generator().make_staging_table_colname(
          _colname);
    };

    col.set_name(helpers::Macros::modify_colnames({col.name()},
                                                  make_staging_table_colname)
                     .at(0));

    _df->add_float_column(col, containers::DataFrame::ROLE_TIME_STAMP);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::add_to_tracker(
    const pipelines::Pipeline& _pipeline,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    containers::DataFrame* _df) {
  const auto dependencies = _pipeline.dependencies();

  const auto build_history = data_frame_tracker().make_build_history(
      dependencies, _population_df, _peripheral_dfs);

  _df->set_build_history(build_history);

  data_frame_tracker().add(*_df);
}

// ------------------------------------------------------------------------

void PipelineManager::check(const std::string& _name,
                            const Poco::JSON::Object& _cmd,
                            Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  // -------------------------------------------------------

  if (pipeline.premium_only()) {
    license_checker().check_enterprise();
  }

  communication::Sender::send_string("Found!", _socket);

  // -------------------------------------------------------

  multithreading::WeakWriteLock weak_write_lock(read_write_lock_);

  const auto pool = options_.make_pool();

  const auto local_categories =
      std::make_shared<containers::Encoding>(pool, categories_);

  const auto local_join_keys_encoding =
      std::make_shared<containers::Encoding>(pool, join_keys_encoding_);

  // -------------------------------------------------------

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding, data_frames_,
                 options_)
          .parse_all(_cmd);

  // -------------------------------------------------------

  const auto params =
      pipelines::CheckParams{.categories_ = local_categories,
                             .cmd_ = _cmd,
                             .logger_ = logger_,
                             .peripheral_dfs_ = peripheral_dfs,
                             .population_df_ = population_df,
                             .preprocessor_tracker_ = preprocessor_tracker_,
                             .warning_tracker_ = warning_tracker_,
                             .socket_ = _socket};

  pipeline.check(params);

  // -------------------------------------------------------

  weak_write_lock.upgrade();

  categories_->append(*local_categories);

  weak_write_lock.unlock();

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::check_user_privileges(
    const pipelines::Pipeline& _pipeline, const std::string& _name,
    const Poco::JSON::Object& _cmd) const {
  if (_pipeline.premium_only()) {
    license_checker().check_enterprise();
  }

  if (JSON::get_value<bool>(_cmd, "http_request_")) {
    if (!_pipeline.allow_http()) {
      throw std::invalid_argument(
          "Pipeline '" + _name +
          "' does not allow HTTP requests. You can activate "
          "this "
          "via the API or the getML monitor!");
    }

    if (!_pipeline.premium_only()) {
      license_checker().check_enterprise();
    }
  }
}

// ------------------------------------------------------------------------

void PipelineManager::column_importances(const std::string& _name,
                                         const Poco::JSON::Object& _cmd,
                                         Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto target_num = JSON::get_value<Int>(_cmd, "target_num_");

  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores();

  // -------------------------------------------------------

  auto importances = std::vector<Float>();

  for (const auto& vec : scores.column_importances()) {
    if (target_num < 0) {
      const auto sum_importances = std::accumulate(vec.begin(), vec.end(), 0.0);

      const auto length = static_cast<Float>(vec.size());

      importances.push_back(sum_importances / length);

      continue;
    }

    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::invalid_argument("target_num out of range!");
    }

    importances.push_back(vec.at(target_num));
  }

  // -------------------------------------------------------

  Poco::JSON::Object response;

  response.set("column_descriptions_",
               JSON::vector_to_array(scores.column_descriptions()));

  response.set("column_importances_", JSON::vector_to_array(importances));

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::deploy(const std::string& _name,
                             const Poco::JSON::Object& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const bool deploy = JSON::get_value<bool>(_cmd, "deploy_");

  auto pipeline = get_pipeline(_name);

  pipeline.allow_http() = deploy;

  set_pipeline(_name, pipeline);

  post_pipeline(pipeline.to_monitor(categories().strings(), _name));

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void PipelineManager::feature_correlations(const std::string& _name,
                                           const Poco::JSON::Object& _cmd,
                                           Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores();

  // -------------------------------------------------------

  auto correlations = std::vector<Float>();

  for (const auto& vec : scores.feature_correlations()) {
    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::invalid_argument("target_num out of range!");
    }

    correlations.push_back(vec.at(target_num));
  }

  // -------------------------------------------------------

  Poco::JSON::Object response;

  response.set("feature_names_", JSON::vector_to_array(scores.feature_names()));

  response.set("feature_correlations_", JSON::vector_to_array(correlations));

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::feature_importances(const std::string& _name,
                                          const Poco::JSON::Object& _cmd,
                                          Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores();

  // -------------------------------------------------------

  auto importances = std::vector<Float>();

  for (const auto& vec : scores.feature_importances()) {
    if (static_cast<size_t>(target_num) >= vec.size()) {
      throw std::invalid_argument("target_num out of range!");
    }

    importances.push_back(vec.at(target_num));
  }

  // -------------------------------------------------------

  Poco::JSON::Object response;

  response.set("feature_names_", JSON::vector_to_array(scores.feature_names()));

  response.set("feature_importances_", JSON::vector_to_array(importances));

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::fit(const std::string& _name,
                          const Poco::JSON::Object& _cmd,
                          Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  auto pipeline = get_pipeline(_name);

  // -------------------------------------------------------

  if (pipeline.premium_only()) {
    license_checker().check_enterprise();
  }

  communication::Sender::send_string("Found!", _socket);

  // -------------------------------------------------------

  multithreading::WeakWriteLock weak_write_lock(read_write_lock_);

  const auto pool = options_.make_pool();

  const auto local_categories =
      std::make_shared<containers::Encoding>(pool, categories_);

  const auto local_join_keys_encoding =
      std::make_shared<containers::Encoding>(pool, join_keys_encoding_);

  // -------------------------------------------------------

  const auto [population_df, peripheral_dfs, validation_df] =
      ViewParser(local_categories, local_join_keys_encoding, data_frames_,
                 options_)
          .parse_all(_cmd);

  // -------------------------------------------------------

  const auto params =
      pipelines::FitParams{.categories_ = local_categories,
                           .cmd_ = _cmd,
                           .data_frames_ = data_frames(),
                           .data_frame_tracker_ = data_frame_tracker(),
                           .fe_tracker_ = fe_tracker_,
                           .logger_ = logger_,
                           .peripheral_dfs_ = peripheral_dfs,
                           .population_df_ = population_df,
                           .pred_tracker_ = pred_tracker_,
                           .preprocessor_tracker_ = preprocessor_tracker_,
                           .validation_df_ = validation_df,
                           .socket_ = _socket};

  pipeline.fit(params);

  // -------------------------------------------------------

  auto it = pipelines().find(_name);

  if (it == pipelines().end()) {
    throw std::runtime_error("Pipeline '" + _name + "' does not exist!");
  }

  // -------------------------------------------------------

  weak_write_lock.upgrade();

  categories_->append(*local_categories);

  it->second = pipeline;

  weak_write_lock.unlock();

  // -------------------------------------------------------

  post_pipeline(pipeline.to_monitor(categories().strings(), _name));

  communication::Sender::send_string("Trained pipeline.", _socket);

  // -------------------------------------------------------
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

    throw std::invalid_argument(msg);
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
  // -------------------------------------------------------

  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores().to_json_obj();

  // -------------------------------------------------------

  Poco::JSON::Object response;

  response.set("proportion_", get_array(scores, "proportion_", target_num));

  response.set("lift_", get_array(scores, "lift_", target_num));

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::post_pipeline(const Poco::JSON::Object& _obj) {
  const auto response = monitor().send_tcp("postpipeline", _obj,
                                           communication::Monitor::TIMEOUT_ON);

  if (response != "Success!") {
    throw std::runtime_error(response);
  }
}

// ------------------------------------------------------------------------

void PipelineManager::precision_recall_curve(const std::string& _name,
                                             const Poco::JSON::Object& _cmd,
                                             Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores().to_json_obj();

  // -------------------------------------------------------

  Poco::JSON::Object response;

  response.set("precision_", get_array(scores, "precision_", target_num));

  response.set("tpr_", get_array(scores, "tpr_", target_num));

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Object PipelineManager::receive_data(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _data_frames,
    Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------
  // Declare local variables. The idea of the local variables
  // is to prevent the global variables from being affected
  // by local data frames.

  multithreading::ReadLock read_lock(read_write_lock_);

  const auto local_read_write_lock =
      std::make_shared<multithreading::ReadWriteLock>();

  auto local_data_frame_manager = DataFrameManager(
      _categories, database_manager_, _data_frames, _join_keys_encoding,
      license_checker_, logger_, monitor_, options_, local_read_write_lock);

  // -------------------------------------------------------
  // Receive data.

  auto cmd = _cmd;

  while (true) {
    const auto name = JSON::get_value<std::string>(cmd, "name_");

    const auto type = JSON::get_value<std::string>(cmd, "type_");

    if (type == "DataFrame") {
      local_data_frame_manager.add_data_frame(name, _socket);
    } else if (type == "DataFrame.from_query") {
      license_checker().check_enterprise();
      local_data_frame_manager.from_query(name, cmd, false, _socket);
    } else if (type == "DataFrame.from_json") {
      license_checker().check_enterprise();
      local_data_frame_manager.from_json(name, cmd, false, _socket);
    } else if (type == "FloatColumn.set_unit") {
      local_data_frame_manager.set_unit(name, cmd, _socket);
    } else if (type == "StringColumn.set_unit") {
      local_data_frame_manager.set_unit_categorical(name, cmd, _socket);
    } else {
      break;
    }

    cmd = communication::Receiver::recv_cmd(logger_, _socket);
  }

  // -------------------------------------------------------

  return cmd;

  // -------------------------------------------------------
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

  multithreading::ReadLock read_lock(read_write_lock_);

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
  Poco::JSON::Object obj;

  obj.set("obj", _pipeline.obj());

  obj.set("scores", get_scores(_pipeline));

  obj.set("targets", JSON::vector_to_array(_pipeline.targets()));

  return obj;
}

// ------------------------------------------------------------------------

void PipelineManager::roc_curve(const std::string& _name,
                                const Poco::JSON::Object& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto target_num = JSON::get_value<unsigned int>(_cmd, "target_num_");

  // -------------------------------------------------------

  const auto pipeline = get_pipeline(_name);

  const auto scores = pipeline.scores().to_json_obj();

  // -------------------------------------------------------

  Poco::JSON::Object response;

  response.set("fpr_", get_array(scores, "fpr_", target_num));

  response.set("tpr_", get_array(scores, "tpr_", target_num));

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(response), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::score(const Poco::JSON::Object& _cmd,
                            const std::string& _name,
                            const containers::DataFrame& _population_df,
                            const containers::NumericalFeatures& _yhat,
                            pipelines::Pipeline* _pipeline,
                            Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------

  const auto population_json = *JSON::get_object(_cmd, "population_df_");

  const auto name = JSON::get_value<std::string>(population_json, "name_");

  const auto scores = _pipeline->score(_population_df, name, _yhat);

  communication::Sender::send_string("Success!", _socket);

  // -------------------------------------------------------

  set_pipeline(_name, *_pipeline);

  post_pipeline(_pipeline->to_monitor(categories().strings(), _name));

  communication::Sender::send_string(JSON::stringify(scores), _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::store_df(
    const pipelines::Pipeline& _pipeline, const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::shared_ptr<containers::Encoding>& _local_categories,
    const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock) {
  _weak_write_lock->upgrade();

  categories_->append(*_local_categories);

  join_keys_encoding_->append(*_local_join_keys_encoding);

  _df->set_categories(categories_);

  _df->set_join_keys_encoding(join_keys_encoding_);

  const auto predict = JSON::get_value<bool>(_cmd, "predict_");

  if (!predict) {
    add_to_tracker(_pipeline, _population_df, _peripheral_dfs, _df);
  }

  data_frames()[_df->name()] = *_df;

  monitor_->send_tcp("postdataframe", _df->to_monitor(),
                     communication::Monitor::TIMEOUT_ON);
}

// ------------------------------------------------------------------------

void PipelineManager::to_db(
    const pipelines::Pipeline& _pipeline, const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_table,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding) {
  const auto df =
      to_df(_pipeline, _cmd, _population_table, _numerical_features,
            _categorical_features, _categories, _join_keys_encoding);

  const auto table_name = JSON::get_value<std::string>(_cmd, "table_name_");

  // We are using the bell character (\a) as the quotechar. It is least likely
  // to appear in any field.
  auto reader = containers::DataFrameReader(df, _categories,
                                            _join_keys_encoding, '\a', '|');

  const auto conn = connector("default");

  assert_true(conn);

  const auto statement = io::StatementMaker::make_statement(
      table_name, conn->dialect(), conn->describe(), reader.colnames(),
      reader.coltypes());

  logger().log(statement);

  conn->execute(statement);

  conn->read(table_name, 0, &reader);

  database_manager_->post_tables();
}

// ------------------------------------------------------------------------

containers::DataFrame PipelineManager::to_df(
    const pipelines::Pipeline& _pipeline, const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_table,
    const containers::NumericalFeatures& _numerical_features,
    const containers::CategoricalFeatures& _categorical_features,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding) {
  const auto df_name = JSON::get_value<std::string>(_cmd, "df_name_");

  const auto pool = options_.make_pool();

  auto df =
      containers::DataFrame(df_name, _categories, _join_keys_encoding, pool);

  if (!_cmd.has("predict_") || !JSON::get_value<bool>(_cmd, "predict_")) {
    add_features_to_df(_pipeline, _numerical_features, _categorical_features,
                       &df);
  } else {
    add_predictions_to_df(_pipeline, _numerical_features, &df);
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

  const auto dialect = JSON::get_value<std::string>(_cmd, "dialect_");

  const auto schema = JSON::get_value<std::string>(_cmd, "schema_");

  multithreading::ReadLock read_lock(read_write_lock_);

  const auto pipeline = get_pipeline(_name);

  const auto sql = pipeline.to_sql(categories().strings(), targets, subfeatures,
                                   dialect, schema);

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

  multithreading::WeakWriteLock weak_write_lock(read_write_lock_);

  const auto pool = options_.make_pool();

  const auto local_categories =
      std::make_shared<containers::Encoding>(pool, categories_);

  const auto local_join_keys_encoding =
      std::make_shared<containers::Encoding>(pool, join_keys_encoding_);

  auto local_data_frames =
      std::make_shared<std::map<std::string, containers::DataFrame>>(
          data_frames());

  auto cmd = communication::Receiver::recv_cmd(logger_, _socket);

  cmd = receive_data(cmd, local_categories, local_join_keys_encoding,
                     local_data_frames, _socket);

  const auto [population_df, peripheral_dfs, _] =
      ViewParser(local_categories, local_join_keys_encoding, local_data_frames,
                 options_)
          .parse_all(cmd);

  // IMPORTANT: Use categories_, not local_categories, otherwise
  // .vector() might not work.
  const auto params =
      pipelines::TransformParams{.categories_ = categories_,
                                 .cmd_ = cmd,
                                 .data_frames_ = *local_data_frames,
                                 .data_frame_tracker_ = data_frame_tracker(),
                                 .logger_ = logger_,
                                 .original_peripheral_dfs_ = peripheral_dfs,
                                 .original_population_df_ = population_df,
                                 .socket_ = _socket};

  const auto [numerical_features, categorical_features] =
      pipeline.transform(params);

  const auto table_name = JSON::get_value<std::string>(cmd, "table_name_");

  const auto df_name = JSON::get_value<std::string>(cmd, "df_name_");

  const auto score = JSON::get_value<bool>(cmd, "score_");

  if (table_name == "" && df_name == "" && !score) {
    communication::Sender::send_string("Success!", _socket);
    communication::Sender::send_features(numerical_features, _socket);
    return;
  }

  if (table_name != "") {
    license_checker().check_enterprise();

    to_db(pipeline, cmd, population_df, numerical_features,
          categorical_features, local_categories, local_join_keys_encoding);
  }

  if (df_name != "") {
    license_checker().check_enterprise();

    auto df =
        to_df(pipeline, cmd, population_df, numerical_features,
              categorical_features, local_categories, local_join_keys_encoding);

    store_df(pipeline, cmd, population_df, peripheral_dfs, local_categories,
             local_join_keys_encoding, &df, &weak_write_lock);
  }

  communication::Sender::send_string("Success!", _socket);

  weak_write_lock.unlock();

  if (score) {
    assert_true(local_data_frames);

    this->score(cmd, _name, population_df, numerical_features, &pipeline,
                _socket);
  }
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
