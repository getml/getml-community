// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_SRV_REQUESTHANDLER_HPP_
#define ENGINE_SRV_REQUESTHANDLER_HPP_

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

#include <atomic>
#include <memory>

#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/DatabaseManager.hpp"
#include "engine/handlers/PipelineManager.hpp"
#include "engine/handlers/ProjectManager.hpp"
#include "engine/handlers/ViewManager.hpp"
#include "fct/Ref.hpp"
#include "fct/define_tagged_union.hpp"

namespace engine {
namespace srv {

/// A RequestHandler handles all request in deploy mode.
class RequestHandler : public Poco::Net::TCPServerConnection {
 public:
  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

 public:
  RequestHandler(const Poco::Net::StreamSocket& _socket,
                 const fct::Ref<handlers::DatabaseManager>& _database_manager,
                 const fct::Ref<handlers::DataFrameManagerParams>& _data_params,
                 const fct::Ref<const communication::Logger>& _logger,
                 const fct::Ref<handlers::PipelineManager>& _pipeline_manager,
                 const config::Options& _options,
                 const fct::Ref<handlers::ProjectManager>& _project_manager,
                 const fct::Ref<std::atomic<bool>>& _shutdown)
      : Poco::Net::TCPServerConnection(_socket),
        database_manager_(_database_manager),
        data_params_(_data_params),
        logger_(_logger),
        pipeline_manager_(_pipeline_manager),
        options_(_options),
        project_manager_(_project_manager),
        shutdown_(_shutdown) {}

  ~RequestHandler() = default;

  /// Required by Poco::Net::TCPServerConnection. Does the actual handling.
  void run();

 private:
  /// Trivial accessor
  handlers::DatabaseManager& database_manager() { return *database_manager_; }

  /// Trivial accessor
  handlers::ColumnManager column_manager() {
    return handlers::ColumnManager(*data_params_);
  }

  /// Trivial accessor
  handlers::DataFrameManager data_frame_manager() {
    return handlers::DataFrameManager(*data_params_);
  }

  /// Trivial accessor
  const communication::Logger& logger() { return *logger_; }

  /// Trivial accessor
  handlers::PipelineManager& pipeline_manager() { return *pipeline_manager_; }

  /// Trivial accessor
  handlers::ProjectManager& project_manager() { return *project_manager_; }

  /// Trivial accessor
  handlers::ViewManager view_manager() {
    return handlers::ViewManager(*data_params_);
  }

 private:
  /// Handles requests related to the database.
  const fct::Ref<handlers::DatabaseManager> database_manager_;

  /// The parameters needed for all of the handlers related to data frames.
  const fct::Ref<handlers::DataFrameManagerParams> data_params_;

  /// Logs commands.
  const fct::Ref<const communication::Logger> logger_;

  /// Handles requests related to a pipeline
  const fct::Ref<handlers::PipelineManager> pipeline_manager_;

  /// Contains information on the port of the monitor process
  const config::Options options_;

  /// Handles requests related to the project as a whole, such as save or
  /// load.
  const fct::Ref<handlers::ProjectManager> project_manager_;

  /// Signals to the main process that we want to shut down.
  const fct::Ref<std::atomic<bool>>& shutdown_;
};

// -----------------------------------------------------------------
}  // namespace srv
}  // namespace engine

#endif  // ENGINE_SRV_REQUESTHANDLER_HPP_
