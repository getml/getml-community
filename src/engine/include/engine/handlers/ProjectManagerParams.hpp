// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_PROJECTMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_PROJECTMANAGERPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/PipelineManagerParams.hpp"
#include "engine/pipelines/pipelines.hpp"

namespace engine {
namespace handlers {

struct ProjectManagerParams {
  typedef PipelineManagerParams::PipelineMapType PipelineMapType;

  /// Maps integers to category names
  const rfl::Ref<containers::Encoding> categories_;

  /// Connector to the underlying database.
  const rfl::Ref<DatabaseManager> database_manager_;

  /// Connector to the underlying database.
  const rfl::Ref<DataFrameManager> data_frame_manager_;

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

  /// The name of the current project
  const std::string project_;

  /// It is sometimes necessary to prevent us from changing the project.
  const rfl::Ref<multithreading::ReadWriteLock> project_lock_;

  /// For coordinating the read and write process of the data
  const rfl::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PROJECTMANAGERPARAMS_HPP_
