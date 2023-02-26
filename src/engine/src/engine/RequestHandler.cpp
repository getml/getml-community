// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/srv/RequestHandler.hpp"

#include "commands/commands.hpp"
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

    const Poco::JSON::Object cmd =
        communication::Receiver::recv_cmd(logger_, &socket());

    const auto basic_command = json::from_json<commands::BasicCommand>(cmd);

    const auto& type = basic_command.get<"type_">();

    const auto& name = basic_command.get<"name_">();

    if (type == "is_alive") {
      return;
    } else if (type == "monitor_url") {
      // The community edition does not have a monitor.
      communication::Sender::send_string("", &socket());
    } else if (type == "Pipeline.check") {
      const auto c = json::from_json<commands::CheckPipeline>(cmd);
      pipeline_manager().check(name, c, &socket());
    } else if (type == "Pipeline.column_importances") {
      pipeline_manager().column_importances(name, cmd, &socket());
    } else if (type == "Pipeline.deploy") {
      pipeline_manager().deploy(name, cmd, &socket());
    } else if (type == "Pipeline.feature_correlations") {
      pipeline_manager().feature_correlations(name, cmd, &socket());
    } else if (type == "Pipeline.feature_importances") {
      pipeline_manager().feature_importances(name, cmd, &socket());
    } else if (type == "Pipeline.fit") {
      pipeline_manager().fit(name, cmd, &socket());
    } else if (type == "Pipeline.lift_curve") {
      pipeline_manager().lift_curve(name, cmd, &socket());
    } else if (type == "Pipeline.precision_recall_curve") {
      pipeline_manager().precision_recall_curve(name, cmd, &socket());
    } else if (type == "Pipeline.refresh") {
      pipeline_manager().refresh(name, &socket());
    } else if (type == "Pipeline.refresh_all") {
      pipeline_manager().refresh_all(&socket());
    } else if (type == "Pipeline.roc_curve") {
      pipeline_manager().roc_curve(name, cmd, &socket());
    } else if (type == "Pipeline.to_sql") {
      pipeline_manager().to_sql(name, cmd, &socket());
    } else if (type == "Pipeline.transform") {
      pipeline_manager().transform(name, cmd, &socket());
    } else if (type == "shutdown") {
      *shutdown_ = true;
    } else {
      const auto obj = json::Parser<commands::DatabaseCommand>::from_json(cmd);
      database_manager().execute_command(obj, &socket());
    }
  } catch (std::exception& e) {
    logger_->log(std::string("Error: ") + e.what());

    communication::Sender::send_string(e.what(), &socket());
  }
}

// ------------------------------------------------------------------------
}  // namespace srv
}  // namespace engine
