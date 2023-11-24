// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/RequestHandler.hpp"

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <stdexcept>

#include "commands/ColumnCommand.hpp"
#include "commands/Command.hpp"
#include "commands/DataFrameCommand.hpp"
#include "commands/DatabaseCommand.hpp"
#include "commands/PipelineCommand.hpp"
#include "commands/ProjectCommand.hpp"
#include "commands/ViewCommand.hpp"
#include "fct/always_false.hpp"
#include "fct/extract_discriminators.hpp"
#include "fct/get.hpp"
#include "json/json.hpp"

namespace engine {
namespace srv {

void RequestHandler::run() {
  try {
    if (socket().peerAddress().host().toString() != "127.0.0.1") {
      throw std::runtime_error("Illegal connection attempt from " +
                               socket().peerAddress().toString() +
                               "! Only connections from localhost "
                               "(127.0.0.1) are allowed!");
    }

    const auto cmd_str = communication::Receiver::recv_cmd(logger_, &socket());

    const auto cmd = json::from_json<commands::Command>(cmd_str);

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
        static_assert(fct::always_false_v<Type>, "Not all cases were covered.");
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