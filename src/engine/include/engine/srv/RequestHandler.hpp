#ifndef ENGINE_SRV_REQUESTHANDLER_HPP_
#define ENGINE_SRV_REQUESTHANDLER_HPP_

// -----------------------------------------------------------------

#if (defined(_WIN32) || defined(_WIN64))
#define _WINSOCKAPI_  // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>

// ----------------------------------------------------
#include <openssl/ossl_typ.h>
#endif

// -----------------------------------------------------------------

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

// -----------------------------------------------------------------

#include <atomic>
#include <memory>

// -----------------------------------------------------------------

#include "debug/debug.hpp"
#include "fct/Ref.hpp"

// -----------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/containers/containers.hpp"
#include "engine/handlers/handlers.hpp"

// -----------------------------------------------------------------

namespace engine {
namespace srv {

/// A RequestHandler handles all request in deploy mode.
class RequestHandler : public Poco::Net::TCPServerConnection {
 public:
  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

  // -------------------------------------------------------------

 public:
  RequestHandler(
      const Poco::Net::StreamSocket& _socket,
      const fct::Ref<handlers::DatabaseManager>& _database_manager,
      const fct::Ref<handlers::DataFrameManager>& _data_frame_manager,
      const fct::Ref<const communication::Logger>& _logger,
      const fct::Ref<handlers::PipelineManager>& _pipeline_manager,
      const config::Options& _options,
      const fct::Ref<handlers::ProjectManager>& _project_manager,
      const fct::Ref<std::atomic<bool>>& _shutdown)
      : Poco::Net::TCPServerConnection(_socket),
        database_manager_(_database_manager),
        data_frame_manager_(_data_frame_manager),
        logger_(_logger),
        pipeline_manager_(_pipeline_manager),
        options_(_options),
        project_manager_(_project_manager),
        shutdown_(_shutdown) {}

  ~RequestHandler() = default;

  /// Required by Poco::Net::TCPServerConnection. Does the actual handling.
  void run();

  // -------------------------------------------------------------

 private:
  /// Trivial accessor
  handlers::DatabaseManager& database_manager() { return *database_manager_; }

  /// Trivial accessor
  handlers::DataFrameManager& data_frame_manager() {
    return *data_frame_manager_;
  }

  /// Trivial accessor
  const communication::Logger& logger() { return *logger_; }

  /// Trivial accessor
  handlers::PipelineManager& pipeline_manager() { return *pipeline_manager_; }

  /// Trivial accessor
  handlers::ProjectManager& project_manager() { return *project_manager_; }

  // -------------------------------------------------------------

 private:
  /// Handles requests related to the database.
  const fct::Ref<handlers::DatabaseManager> database_manager_;

  /// Handles requests related to the data frames.
  const fct::Ref<handlers::DataFrameManager> data_frame_manager_;

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
