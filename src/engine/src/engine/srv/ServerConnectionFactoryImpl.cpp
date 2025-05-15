// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/ServerConnectionFactoryImpl.hpp"

#include "engine/srv/RequestHandler.hpp"

#include <cstdlib>

namespace engine::srv {

ServerConnectionFactoryImpl::ServerConnectionFactoryImpl(
    const rfl::Ref<handlers::DatabaseManager>& _database_manager,
    const rfl::Ref<handlers::DataFrameManagerParams>& _data_params,
    const rfl::Ref<const communication::Logger>& _logger,
    const config::Options& _options,
    const rfl::Ref<handlers::PipelineManager>& _pipeline_manager,
    const rfl::Ref<handlers::ProjectManager>& _project_manager,
    const rfl::Ref<std::atomic<bool>>& _shutdown)
    : database_manager_(_database_manager),
      data_params_(_data_params),
      logger_(_logger),
      options_(_options),
      pipeline_manager_(_pipeline_manager),
      project_manager_(_project_manager),
      shutdown_(_shutdown) {}

Poco::Net::TCPServerConnection* ServerConnectionFactoryImpl::createConnection(
    const Poco::Net::StreamSocket& _socket) {
  return new RequestHandler(_socket, database_manager_, data_params_, logger_,
                            pipeline_manager_, options_, project_manager_,
                            shutdown_);
}
}  // namespace engine::srv
