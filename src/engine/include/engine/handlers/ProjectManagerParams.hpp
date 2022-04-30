#ifndef ENGINE_HANDLERS_PROJECTMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_PROJECTMANAGERPARAMS_HPP_

// ------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ------------------------------------------------------------------------

#include <map>
#include <memory>
#include <string>

// ------------------------------------------------------------------------

#include "debug/debug.hpp"

// ------------------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/containers/containers.hpp"
#include "engine/hyperparam/hyperparam.hpp"
#include "engine/licensing/licensing.hpp"
#include "engine/pipelines/pipelines.hpp"

// ------------------------------------------------------------------------

#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/PipelineManagerParams.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace handlers {

struct ProjectManagerParams {
  typedef PipelineManagerParams::PipelineMapType PipelineMapType;

  /// Maps integers to category names
  const fct::Ref<containers::Encoding> categories_;

  /// Connector to the underlying database.
  const fct::Ref<DatabaseManager> database_manager_;

  /// Connector to the underlying database.
  const fct::Ref<DataFrameManager> data_frame_manager_;

  /// The data frames currently held in memory
  const fct::Ref<std::map<std::string, containers::DataFrame>> data_frames_;

  /// Keeps track of all data frames, so we don't have to
  /// reconstruct the features all of the time.
  const fct::Ref<dependency::DataFrameTracker> data_frame_tracker_;

  /// Keeps track of all feature learners.
  const fct::Ref<dependency::FETracker> fe_tracker_;

  const fct::Ref<std::map<std::string, hyperparam::Hyperopt>>& hyperopts_;

  /// Maps integers to join key names
  const fct::Ref<containers::Encoding> join_keys_encoding_;

  /// For checking the number of cores and memory usage
  const fct::Ref<licensing::LicenseChecker> license_checker_;

  /// For logging
  const fct::Ref<const communication::Logger> logger_;

  /// For communication with the monitor
  const fct::Ref<const communication::Monitor> monitor_;

  /// Settings for the engine and the monitor
  const config::Options options_;

  /// The pipelines currently held in memory
  const fct::Ref<PipelineMapType> pipelines_;

  /// Keeps track of all predictors.
  const fct::Ref<dependency::PredTracker> pred_tracker_;

  /// Keeps track of all preprocessors.
  const fct::Ref<dependency::PreprocessorTracker> preprocessor_tracker_;

  /// The name of the current project
  const std::string project_;

  /// It is sometimes necessary to prevent us from changing the project.
  const fct::Ref<multithreading::ReadWriteLock> project_lock_;

  /// For coordinating the read and write process of the data
  const fct::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PROJECTMANAGERPARAMS_HPP_
