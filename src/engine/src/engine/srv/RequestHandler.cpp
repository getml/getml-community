// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/RequestHandler.hpp"

#include "commands/ColumnCommand.hpp"
#include "commands/Command.hpp"
#include "commands/DataFrameCommand.hpp"
#include "commands/DatabaseCommand.hpp"
#include "commands/PipelineCommand.hpp"
#include "commands/ProjectCommand.hpp"
#include "commands/ViewCommand.hpp"

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <rfl/always_false.hpp>
#include <rfl/extract_discriminators.hpp>
#include <rfl/get.hpp>
#include <rfl/json/read.hpp>

#include <stdexcept>

namespace engine {
namespace srv {

RequestHandler::RequestHandler(
    const Poco::Net::StreamSocket& _socket,
    const rfl::Ref<handlers::DatabaseManager>& _database_manager,
    const rfl::Ref<handlers::DataFrameManagerParams>& _data_params,
    const rfl::Ref<const communication::Logger>& _logger,
    const rfl::Ref<handlers::PipelineManager>& _pipeline_manager,
    const config::Options& _options,
    const rfl::Ref<handlers::ProjectManager>& _project_manager,
    const rfl::Ref<std::atomic<bool>>& _shutdown)
    : Poco::Net::TCPServerConnection(_socket),
      database_manager_(_database_manager),
      data_params_(_data_params),
      logger_(_logger),
      pipeline_manager_(_pipeline_manager),
      options_(_options),
      project_manager_(_project_manager),
      shutdown_(_shutdown) {}

void RequestHandler::run() {
  try {
    if (socket().peerAddress().host().toString() != "127.0.0.1") {
      throw std::runtime_error("Illegal connection attempt from " +
                               socket().peerAddress().toString() +
                               "! Only connections from localhost "
                               "(127.0.0.1) are allowed!");
    }

    const auto cmd_str = communication::Receiver::recv_cmd(logger_, &socket());

    const auto cmd = rfl::json::read<commands::Command>(cmd_str).value();

    const auto handle = [this](const auto& _cmd) {
      using Type = std::decay_t<decltype(_cmd)>;
      if constexpr (std::is_same<Type, commands::ColumnCommand>()) {
        return column_manager().execute_command(_cmd, &socket());
      } else if constexpr (std::is_same<Type, commands::DatabaseCommand>()) {
        return database_manager().execute_command(_cmd, &socket());
      } else if constexpr (std::is_same<Type, commands::DataFrameCommand>()) {
        return data_frame_manager().execute_command(_cmd, &socket());
      } else if constexpr (std::is_same<Type, commands::PipelineCommand>()) {
        return pipeline_manager().execute_command(_cmd, &socket());
      } else if constexpr (std::is_same<Type, commands::ProjectCommand>()) {
        return project_manager().execute_command(_cmd, &socket());
      } else if constexpr (std::is_same<Type, commands::ViewCommand>()) {
        return view_manager().execute_command(_cmd, &socket());
      } else if constexpr (std::is_same<
                               Type, typename commands::Command::IsAliveOp>()) {
        return;
      } else if constexpr (std::is_same<
                               Type,
                               typename commands::Command::MonitorURLOp>()) {
        // the community edition does not have a monitor.
        communication::Sender::send_string("", &socket());
      } else if constexpr (std::is_same<
                               Type,
                               typename commands::Command::ShutdownOp>()) {
        *shutdown_ = true;
      } else {
        static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
      }
    };

    std::visit(handle, cmd.val_);

  } catch (std::exception& e) {
    logger_->log(std::string("Error: ") + e.what());

    communication::Sender::send_string(e.what(), &socket());
  }
}
}  // namespace srv
}  // namespace engine
