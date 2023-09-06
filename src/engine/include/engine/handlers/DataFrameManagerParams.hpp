// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_DATAFRAMEMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_DATAFRAMEMANAGERPARAMS_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/DatabaseManager.hpp"

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
