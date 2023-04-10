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

#include "commands/AddDfCommand.hpp"
#include "commands/ColumnCommand.hpp"
#include "commands/DataFrameCommand.hpp"
#include "commands/DatabaseCommand.hpp"
#include "commands/PipelineCommand.hpp"
#include "commands/ProjectCommand.hpp"
#include "commands/ViewCommand.hpp"
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

    const auto obj =
        Poco::JSON::Parser().parse(cmd_str).extract<Poco::JSON::Object::Ptr>();

    if (!obj) {
      throw std::runtime_error("Could not parse JSON!");
    }

    const auto type = obj->get("type_").toString();

    using AddDfLiteral = fct::extract_discriminators_t<
        typename commands::AddDfCommand::NamedTupleType>;

    using ColumnLiteral = fct::extract_discriminators_t<
        typename commands::ColumnCommand::NamedTupleType>;

    using DatabaseLiteral = fct::extract_discriminators_t<
        typename commands::DatabaseCommand::NamedTupleType>;

    using DataFrameLiteral = fct::extract_discriminators_t<
        typename commands::DataFrameCommand::NamedTupleType>;

    using PipelineLiteral = fct::extract_discriminators_t<
        typename commands::PipelineCommand::NamedTupleType>;

    using ProjectLiteral = fct::extract_discriminators_t<
        typename commands::ProjectCommand::NamedTupleType>;

    using ViewLiteral = fct::extract_discriminators_t<
        typename commands::ViewCommand::NamedTupleType>;

    if (AddDfLiteral::contains(type)) {
      const auto cmd = commands::AddDfCommand::from_json(*obj);
      project_manager().execute_add_df_command(cmd, &socket());
    } else if (ColumnLiteral::contains(type)) {
      const auto cmd = commands::ColumnCommand::from_json(*obj);
      column_manager().execute_command(cmd, &socket());
    } else if (DatabaseLiteral::contains(type)) {
      const auto cmd = commands::DatabaseCommand::from_json(*obj);
      database_manager().execute_command(cmd, &socket());
    } else if (DataFrameLiteral::contains(type)) {
      const auto cmd = commands::DataFrameCommand::from_json(*obj);
      data_frame_manager().execute_command(cmd, &socket());
    } else if (PipelineLiteral::contains(type)) {
      const auto cmd = commands::PipelineCommand::from_json(*obj);
      pipeline_manager().execute_command(cmd, &socket());
    } else if (ProjectLiteral::contains(type)) {
      const auto cmd = commands::ProjectCommand::from_json(*obj);
      project_manager().execute_command(cmd, &socket());
    } else if (ViewLiteral::contains(type)) {
      const auto cmd = commands::ViewCommand::from_json(*obj);
      view_manager().execute_command(cmd, &socket());
    } else if (type == "is_alive") {
      return;
    } else if (type == "monitor_url") {
      // The community edition does not have a monitor.
      communication::Sender::send_string("", &socket());
    } else if (type == "shutdown") {
      *shutdown_ = true;
    } else {
      throw std::runtime_error("Unknown type: '" + type + "'!");
    }
  } catch (std::exception& e) {
    logger_->log(std::string("Error: ") + e.what());

    communication::Sender::send_string(e.what(), &socket());
  }
}

}  // namespace srv
}  // namespace engine
