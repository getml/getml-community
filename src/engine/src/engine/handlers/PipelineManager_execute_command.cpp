// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "containers/Roles.hpp"
#include "engine/handlers/PipelineManager.hpp"

#include <rfl/always_false.hpp>

namespace engine {
namespace handlers {

void PipelineManager::execute_command(const Command& _command,
                                      Poco::Net::StreamSocket* _socket) {
  const auto handle = [this, _socket](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, Command::CheckOp>()) {
      check(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ColumnImportancesOp>()) {
      column_importances(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::DeployOp>()) {
      deploy(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::FeatureCorrelationsOp>()) {
      feature_correlations(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::FeatureImportancesOp>()) {
      feature_importances(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::FitOp>()) {
      fit(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::LiftCurveOp>()) {
      lift_curve(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::PrecisionRecallCurveOp>()) {
      precision_recall_curve(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::RefreshOp>()) {
      refresh(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::RefreshAllOp>()) {
      refresh_all(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ROCCurveOp>()) {
      roc_curve(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ToSQLOp>()) {
      to_sql(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::TransformOp>()) {
      transform(_cmd, _socket);
    } else {
      static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  rfl::visit(handle, _command.val_);
}

}  // namespace handlers
}  // namespace engine
