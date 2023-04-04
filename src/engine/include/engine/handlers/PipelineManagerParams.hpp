// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "debug/debug.hpp"
#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/DatabaseManager.hpp"
#include "engine/handlers/ViewParser.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/pipelines.hpp"

namespace engine {
namespace handlers {

struct PipelineManagerParams {
  typedef std::map<std::string, pipelines::Pipeline> PipelineMapType;

  /// Maps integers to category names
  const fct::Ref<containers::Encoding> categories_;

  /// Connector to the underlying database.
  const fct::Ref<DatabaseManager> database_manager_;

  /// The data frames currently held in memory
  const fct::Ref<std::map<std::string, containers::DataFrame>> data_frames_;

  /// Keeps track of all data frames, so we don't have to
  /// reconstruct the features all of the time.
  const fct::Ref<dependency::DataFrameTracker> data_frame_tracker_;

  /// Keeps track of all feature learners.
  const fct::Ref<dependency::FETracker> fe_tracker_;

  /// Maps integers to join key names
  const fct::Ref<containers::Encoding> join_keys_encoding_;

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

  /// For coordinating the read and write process of the data
  const fct::Ref<multithreading::ReadWriteLock> read_write_lock_;

  /// Keeps track of all warnings.
  const fct::Ref<dependency::WarningTracker> warning_tracker_;
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_
