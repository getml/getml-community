// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <stdexcept>

#include "commands/DataFramesOrViews.hpp"
#include "commands/ProjectCommand.hpp"
#include "engine/containers/Roles.hpp"
#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/PipelineManager.hpp"
#include "engine/pipelines/ToSQL.hpp"
#include "engine/pipelines/ToSQLParams.hpp"
#include "engine/pipelines/pipelines.hpp"
#include "fct/always_false.hpp"
#include "transpilation/TranspilationParams.hpp"
#include "transpilation/transpilation.hpp"

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
      static_assert(fct::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  fct::visit(handle, _command.val_);
}

}  // namespace handlers
}  // namespace engine
