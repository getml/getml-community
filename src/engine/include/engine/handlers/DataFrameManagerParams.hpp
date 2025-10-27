// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_DATAFRAMEMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_DATAFRAMEMANAGERPARAMS_HPP_

#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "engine/handlers/DatabaseManager.hpp"

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <string>

namespace engine {
namespace handlers {

struct DataFrameManagerParams {
  /// Maps integeres to category names
  const rfl::Ref<containers::Encoding> categories_;

  /// Connector to the underlying database.
  const rfl::Ref<DatabaseManager> database_manager_;

  /// The data frames currently held in memory
  const rfl::Ref<std::map<std::string, containers::DataFrame>> data_frames_;

  /// Maps integers to join key names
  const rfl::Ref<containers::Encoding> join_keys_encoding_;

  /// For logging
  const rfl::Ref<const communication::Logger> logger_;

  /// For communication with the monitor
  const rfl::Ref<const communication::Monitor> monitor_;

  /// Settings for the engine and the monitor
  const config::Options options_;

  /// For coordinating the read and write process of the data
  const rfl::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_
