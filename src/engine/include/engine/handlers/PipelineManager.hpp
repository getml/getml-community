// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_PIPELINEMANAGER_HPP_
#define ENGINE_HANDLERS_PIPELINEMANAGER_HPP_

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "commands/PipelineCommand.hpp"
#include "debug/debug.hpp"
#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/DatabaseManager.hpp"
#include "engine/handlers/PipelineManagerParams.hpp"
#include "engine/handlers/ViewParser.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/pipelines.hpp"

namespace engine {
namespace handlers {

class PipelineManager {
 public:
  typedef std::map<std::string, pipelines::Pipeline> PipelineMapType;

  using Command = commands::PipelineCommand;

 public:
  PipelineManager(const PipelineManagerParams& _params) : params_(_params) {}

  ~PipelineManager() = default;

 public:
  /// Executes a PipelineCommand.
  void execute_command(const Command& _command,
                       Poco::Net::StreamSocket* _socket);

 private:
  /// Checks the validity of the data model.
  void check(const typename Command::CheckOp& _cmd,
             Poco::Net::StreamSocket* _socket);

  /// Returns the column importances of a pipeline.
  void column_importances(const typename Command::ColumnImportancesOp& _cmd,
                          Poco::Net::StreamSocket* _socket);

  /// Determines whether the pipeline should
  /// allow HTTP requests.
  void deploy(const typename Command::DeployOp& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Returns the feature correlations of a pipeline.
  void feature_correlations(const typename Command::FeatureCorrelationsOp& _cmd,
                            Poco::Net::StreamSocket* _socket);

  /// Returns the feature importances of a pipeline.
  void feature_importances(const typename Command::FeatureImportancesOp& _cmd,
                           Poco::Net::StreamSocket* _socket);

  /// Fits a pipeline
  void fit(const typename Command::FitOp& _cmd,
           Poco::Net::StreamSocket* _socket);

  /// Sends a command to the monitor to launch a hyperparameter optimization.
  void launch_hyperopt(const std::string& _name,
                       Poco::Net::StreamSocket* _socket);

  /// Writes a JSON representation of the lift curve into the socket.
  void lift_curve(const typename Command::LiftCurveOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Writes a JSON representation of the precision-recall curve into the
  /// socket.
  void precision_recall_curve(
      const typename Command::PrecisionRecallCurveOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Refreshes a pipeline in the target language
  void refresh(const typename Command::RefreshOp& _cmd,
               Poco::Net::StreamSocket* _socket);

  /// Refreshes all pipeline in the target language
  void refresh_all(const typename Command::RefreshAllOp& _cmd,
                   Poco::Net::StreamSocket* _socket);

  /// Writes a JSON representation of the ROC curve into the socket.
  void roc_curve(const typename Command::ROCCurveOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Transform a pipeline to a JSON string
  void to_json(const std::string& _name, Poco::Net::StreamSocket* _socket);

  /// Extracts the SQL code
  void to_sql(const typename Command::ToSQLOp& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Generate features
  void transform(const typename Command::TransformOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  // ------------------------------------------------------------------------

 private:
  /// Adds a pipeline's features to the data frame.
  void add_features_to_df(
      const pipelines::FittedPipeline& _fitted,
      const containers::NumericalFeatures& _numerical_features,
      const containers::CategoricalFeatures& _categorical_features,
      containers::DataFrame* _df) const;

  /// Adds the join keys from the population table to the data frame.
  void add_join_keys_to_df(const containers::DataFrame& _population_table,
                           containers::DataFrame* _df) const;

  /// Adds a pipeline's predictions to the data frame.
  void add_predictions_to_df(
      const pipelines::FittedPipeline& _fitted,
      const containers::NumericalFeatures& _numerical_features,
      containers::DataFrame* _df) const;

  /// Adds the join keys from the population table to the data frame.
  void add_time_stamps_to_df(const containers::DataFrame& _population_table,
                             containers::DataFrame* _df) const;

  /// Adds a data frame to the data frame tracker.
  void add_to_tracker(const pipelines::FittedPipeline& _fitted,
                      const containers::DataFrame& _population_df,
                      const std::vector<containers::DataFrame>& _peripheral_dfs,
                      containers::DataFrame* _df);

  /// Makes sure that the user is allowed to transform this pipeline.
  void check_user_privileges(
      const pipelines::Pipeline& _pipeline, const std::string& _name,
      const fct::NamedTuple<fct::Field<"http_request_", bool>>& _cmd) const;

  /// Retrieves the scores from the pipeline, adding set_used if available.
  Poco::JSON::Object get_scores(const pipelines::Pipeline& _pipeline) const;

  /// Receives data from the client. This data will not be stored permanently,
  /// but locally. Once the training/transformation process is complete, it
  /// will be deleted.
  typename Command::TransformOp receive_data(
      const typename Command::TransformOp& _cmd,
      const fct::Ref<containers::Encoding>& _categories,
      const fct::Ref<containers::Encoding>& _join_keys_encoding,
      const fct::Ref<std::map<std::string, containers::DataFrame>>&
          _data_frames,
      Poco::Net::StreamSocket* _socket);

  /// Returns the data needed for refreshing a single pipeline.
  Poco::JSON::Object refresh_pipeline(
      const pipelines::Pipeline& _pipeline) const;

  /// Under some circumstances, we might want to send data to the client, such
  /// as targets from the population or the results of a transform call.
  void send_data(const fct::Ref<containers::Encoding>& _categories,
                 const fct::Ref<std::map<std::string, containers::DataFrame>>&
                     _local_data_frames,
                 Poco::Net::StreamSocket* _socket);

  /// Scores a pipeline
  void score(const typename Command::TransformOp& _cmd,
             const std::string& _name,
             const containers::DataFrame& _population_df,
             const containers::NumericalFeatures& _yhat,
             const pipelines::Pipeline& _pipeline,
             Poco::Net::StreamSocket* _socket);

  /// Stores the newly created data frame.
  void store_df(const pipelines::FittedPipeline& _fitted,
                const typename Command::TransformOp& _cmd,
                const containers::DataFrame& _population_df,
                const std::vector<containers::DataFrame>& _peripheral_dfs,
                const fct::Ref<containers::Encoding>& _local_categories,
                const fct::Ref<containers::Encoding>& _local_join_keys_encoding,
                containers::DataFrame* _df,
                multithreading::WeakWriteLock* _weak_write_lock);

  /// Writes a set of features to the data base.
  void to_db(const pipelines::FittedPipeline& _fitted,
             const typename Command::TransformOp& _cmd,
             const containers::DataFrame& _population_table,
             const containers::NumericalFeatures& _numerical_features,
             const containers::CategoricalFeatures& _categorical_features,
             const fct::Ref<containers::Encoding>& _categories,
             const fct::Ref<containers::Encoding>& _join_keys_encoding);

  /// Writes a set of features to a DataFrame.
  containers::DataFrame to_df(
      const pipelines::FittedPipeline& _fitted,
      const typename Command::TransformOp& _cmd,
      const containers::DataFrame& _population_table,
      const containers::NumericalFeatures& _numerical_features,
      const containers::CategoricalFeatures& _categorical_features,
      const fct::Ref<containers::Encoding>& _categories,
      const fct::Ref<containers::Encoding>& _join_keys_encoding);

  // ------------------------------------------------------------------------

 private:
  /// Trivial accessor
  const containers::Encoding& categories() const {
    return *params_.categories_;
  }

  /// Trivial accessor
  fct::Ref<database::Connector> connector(const std::string& _name) {
    return params_.database_manager_->connector(_name);
  }

  /// Trivial accessor
  std::map<std::string, containers::DataFrame>& data_frames() {
    return *params_.data_frames_;
  }

  /// Trivial accessor
  dependency::DataFrameTracker& data_frame_tracker() {
    return *params_.data_frame_tracker_;
  }

  /// Returns a deep copy of a pipeline.
  pipelines::Pipeline get_pipeline(const std::string& _name) {
    multithreading::ReadLock read_lock(params_.read_write_lock_);
    const auto& p = utils::Getter::get(_name, &pipelines());
    return p;
  }

  /// Trivial (private) accessor
  const communication::Logger& logger() { return *params_.logger_; }

  /// Trivial (private) accessor
  const communication::Monitor& monitor() { return *params_.monitor_; }

  /// Trivial (private) accessor
  PipelineMapType& pipelines() { return *params_.pipelines_; }

  /// Trivial (private) accessor
  const PipelineMapType& pipelines() const {
    multithreading::ReadLock read_lock(params_.read_write_lock_);
    return *params_.pipelines_;
  }

  /// Sets a pipeline.
  void set_pipeline(const std::string& _name,
                    const pipelines::Pipeline& _pipeline) {
    multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

    auto it = pipelines().find(_name);

    if (it == pipelines().end()) {
      throw std::runtime_error("Pipeline '" + _name + "' does not exist!");
    }

    weak_write_lock.upgrade();

    it->second = _pipeline;
  }

  // ------------------------------------------------------------------------

 private:
  /// The underlying parameters.
  const PipelineManagerParams params_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PIPELINEMANAGER_HPP_
