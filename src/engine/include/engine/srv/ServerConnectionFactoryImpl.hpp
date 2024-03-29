// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_
#define ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnectionFactory.h>

#include <atomic>
#include <memory>

#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/handlers.hpp"
#include "engine/srv/RequestHandler.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace srv {

class ServerConnectionFactoryImpl
    : public Poco::Net::TCPServerConnectionFactory {
 public:
  ServerConnectionFactoryImpl(
      const fct::Ref<handlers::DatabaseManager>& _database_manager,
      const fct::Ref<handlers::DataFrameManagerParams>& _data_params,
      const fct::Ref<const communication::Logger>& _logger,
      const config::Options& _options,
      const fct::Ref<handlers::PipelineManager>& _pipeline_manager,
      const fct::Ref<handlers::ProjectManager>& _project_manager,
      const fct::Ref<std::atomic<bool>>& _shutdown)
      : database_manager_(_database_manager),
        data_params_(_data_params),
        logger_(_logger),
        options_(_options),
        pipeline_manager_(_pipeline_manager),
        project_manager_(_project_manager),
        shutdown_(_shutdown) {}

  /// Required by Poco::Net::TCPServerConnectionFactory. Does the actual
  /// handling.
  Poco::Net::TCPServerConnection* createConnection(
      const Poco::Net::StreamSocket& _socket) {
    return new RequestHandler(_socket, database_manager_, data_params_, logger_,
                              pipeline_manager_, options_, project_manager_,
                              shutdown_);
  }

 private:
  /// Handles requests related to the database.
  const fct::Ref<handlers::DatabaseManager> database_manager_;

  /// Handles requests related to the data frames.
  const fct::Ref<handlers::DataFrameManagerParams> data_params_;

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
};

}  // namespace srv
}  // namespace engine

#endif  // ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_
