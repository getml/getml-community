#ifndef ENGINE_HANDLERS_DATAFRAMEMANAGERPARAMS_HPP_
#define ENGINE_HANDLERS_DATAFRAMEMANAGERPARAMS_HPP_

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
#include "engine/licensing/licensing.hpp"

// ------------------------------------------------------------------------

#include "engine/handlers/DatabaseManager.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace handlers {

struct DataFrameManagerParams {
  /// Maps integeres to category names
  const fct::Ref<containers::Encoding> categories_;

  /// Connector to the underlying database.
  const fct::Ref<DatabaseManager> database_manager_;

  /// The data frames currently held in memory
  const fct::Ref<std::map<std::string, containers::DataFrame>> data_frames_;

  /// Maps integers to join key names
  const fct::Ref<containers::Encoding> join_keys_encoding_;

  /// For checking the license and memory usage
  const fct::Ref<licensing::LicenseChecker> license_checker_;

  /// For logging
  const fct::Ref<const communication::Logger> logger_;

  /// For communication with the monitor
  const fct::Ref<const communication::Monitor> monitor_;

  /// Settings for the engine and the monitor
  const config::Options options_;

  /// For coordinating the read and write process of the data
  const fct::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PIPELINEMANAGERPARAMS_HPP_
