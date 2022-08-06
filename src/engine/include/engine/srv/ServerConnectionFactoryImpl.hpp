// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_
#define ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_

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
#include <Poco/Net/TCPServerConnectionFactory.h>

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

#include "engine/srv/RequestHandler.hpp"

// -----------------------------------------------------------------

namespace engine {
namespace srv {

class ServerConnectionFactoryImpl
    : public Poco::Net::TCPServerConnectionFactory {
 public:
  ServerConnectionFactoryImpl(
      const fct::Ref<handlers::DatabaseManager>& _database_manager,
      const fct::Ref<handlers::DataFrameManager>& _data_frame_manager,
      const fct::Ref<const communication::Logger>& _logger,
      const config::Options& _options,
      const fct::Ref<handlers::PipelineManager>& _pipeline_manager,
      const fct::Ref<handlers::ProjectManager>& _project_manager,
      const fct::Ref<std::atomic<bool>>& _shutdown)
      : database_manager_(_database_manager),
        data_frame_manager_(_data_frame_manager),
        logger_(_logger),
        options_(_options),
        pipeline_manager_(_pipeline_manager),
        project_manager_(_project_manager),
        shutdown_(_shutdown) {}

  /// Required by Poco::Net::TCPServerConnectionFactory. Does the actual
  /// handling.
  Poco::Net::TCPServerConnection* createConnection(
      const Poco::Net::StreamSocket& _socket) {
    return new RequestHandler(_socket, database_manager_, data_frame_manager_,
                              logger_, pipeline_manager_, options_,
                              project_manager_, shutdown_);
  }

  // -------------------------------------------------------------

 private:
  /// Handles requests related to the database.
  const fct::Ref<handlers::DatabaseManager> database_manager_;

  /// Handles requests related to the data frames.
  const fct::Ref<handlers::DataFrameManager> data_frame_manager_;

  /// Logs commands.
  const fct::Ref<const communication::Logger> logger_;

  /// Contains information on the port of the monitor process
  const config::Options options_;

  /// Handles requests related to pipelines.
  const fct::Ref<handlers::PipelineManager> pipeline_manager_;

  /// Handles requests related to the project as a whole, such as save or
  /// load.
  const fct::Ref<handlers::ProjectManager> project_manager_;

  /// Signals to the main process that we want to shut down.
  const fct::Ref<std::atomic<bool>>& shutdown_;

  // -------------------------------------------------------------
};

// -----------------------------------------------------------------
}  // namespace srv
}  // namespace engine

#endif  // ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_
