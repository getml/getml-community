// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_

#include "engine/dependency/DataFrameTracker.hpp"
#include "engine/dependency/FETracker.hpp"
#include "engine/dependency/PredTracker.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"
#include "engine/dependency/WarningTracker.hpp"
#include "engine/handlers/DatabaseManager.hpp"
#include "engine/pipelines/Pipeline.hpp"

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <string>

namespace engine {
namespace handlers {

struct PipelineManagerParams {
  typedef std::map<std::string, pipelines::Pipeline> PipelineMapType;

  /// Maps integers to category names
  const rfl::Ref<containers::Encoding> categories_;

  /// Connector to the underlying database.
  const rfl::Ref<DatabaseManager> database_manager_;

  /// The data frames currently held in memory
  const rfl::Ref<std::map<std::string, containers::DataFrame>> data_frames_;

  /// Keeps track of all data frames, so we don't have to
  /// reconstruct the features all of the time.
  const rfl::Ref<dependency::DataFrameTracker> data_frame_tracker_;

  /// Keeps track of all feature learners.
  const rfl::Ref<dependency::FETracker> fe_tracker_;

  /// Maps integers to join key names
  const rfl::Ref<containers::Encoding> join_keys_encoding_;

  /// For logging
  const rfl::Ref<const communication::Logger> logger_;

  /// For communication with the monitor
  const rfl::Ref<const communication::Monitor> monitor_;

  /// Settings for the engine and the monitor
  const config::Options options_;

  /// The pipelines currently held in memory
  const rfl::Ref<PipelineMapType> pipelines_;

  /// Keeps track of all predictors.
  const rfl::Ref<dependency::PredTracker> pred_tracker_;

  /// Keeps track of all preprocessors.
  const rfl::Ref<dependency::PreprocessorTracker> preprocessor_tracker_;

  /// For coordinating the read and write process of the data
  const rfl::Ref<multithreading::ReadWriteLock> read_write_lock_;

  /// Keeps track of all warnings.
  const rfl::Ref<dependency::WarningTracker> warning_tracker_;
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_
