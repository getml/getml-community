// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/RequestHandler.hpp"

#include "commands/commands.hpp"
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

    const auto command = recv_cmd();

    const auto handle = [this](const auto& _cmd) {
      // Note that all of these four commands are actually tagged unions
      // themselves.

      using DatabaseLiteral = fct::extract_discriminators_t<
          typename commands::DatabaseCommand::NamedTupleType>;

      using DataFrameLiteral = fct::extract_discriminators_t<
          typename commands::DataFrameCommand::NamedTupleType>;

      using PipelineLiteral = fct::extract_discriminators_t<
          typename commands::PipelineCommand::NamedTupleType>;

      using ProjectLiteral = fct::extract_discriminators_t<
          typename commands::ProjectCommand::NamedTupleType>;

      using CmdType = std::decay_t<decltype(fct::get<"type_">(_cmd))>;

      if constexpr (CmdType::template contains_any<DatabaseLiteral>()) {
        database_manager().execute_command(_cmd, &socket());
      } else if constexpr (CmdType::template contains_any<DataFrameLiteral>()) {
        data_frame_manager().execute_command(_cmd, &socket());
      } else if constexpr (CmdType::template contains_any<PipelineLiteral>()) {
        pipeline_manager().execute_command(_cmd, &socket());
      } else if constexpr (CmdType::template contains_any<ProjectLiteral>()) {
        project_manager().execute_command(_cmd, &socket());
      } else {
        []<bool _flag = false>() {
          static_assert(_flag, "Not all cases were covered.");
        }
        ();
      }
    };

    fct::visit(handle, command);

    /*if (type == "is_alive") {
      return;
    } else if (type == "monitor_url") {
      // The community edition does not have a monitor.
      communication::Sender::send_string("", &socket());
    } else if (type == "shutdown") {
      *shutdown_ = true;
    }*/
  } catch (std::exception& e) {
    logger_->log(std::string("Error: ") + e.what());

    communication::Sender::send_string(e.what(), &socket());
  }
}

// ------------------------------------------------------------------------
}  // namespace srv
}  // namespace engine
