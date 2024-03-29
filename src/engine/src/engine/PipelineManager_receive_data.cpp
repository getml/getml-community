// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <stdexcept>

#include "commands/DataFramesOrViews.hpp"
#include "commands/ProjectCommand.hpp"
#include "containers/Roles.hpp"
#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/PipelineManager.hpp"
#include "engine/pipelines/pipelines.hpp"
#include "rfl/always_false.hpp"
#include "transpilation/TranspilationParams.hpp"
#include "transpilation/transpilation.hpp"

namespace engine {
namespace handlers {

typename PipelineManager::FullTransformOp PipelineManager::receive_data(
    const typename Command::TransformOp& _cmd,
    const rfl::Ref<containers::Encoding>& _categories,
    const rfl::Ref<containers::Encoding>& _join_keys_encoding,
    const rfl::Ref<std::map<std::string, containers::DataFrame>>& _data_frames,
    Poco::Net::StreamSocket* _socket) {
  // Declare local variables. The idea of the local variables
  // is to prevent the global variables from being affected
  // by local data frames.

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto local_read_write_lock =
      rfl::Ref<multithreading::ReadWriteLock>::make();

  const auto data_frame_manager_params =
      DataFrameManagerParams{.categories_ = _categories,
                             .database_manager_ = params_.database_manager_,
                             .data_frames_ = _data_frames,
                             .join_keys_encoding_ = _join_keys_encoding,
                             .logger_ = params_.logger_,
                             .monitor_ = params_.monitor_,
                             .options_ = params_.options_,
                             .read_write_lock_ = local_read_write_lock};

  auto local_data_frame_manager = DataFrameManager(data_frame_manager_params);

  auto local_column_manager = ColumnManager(data_frame_manager_params);

  using DataFrameCmd =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame">>,
                      rfl::Field<"name_", std::string>>;

  using CmdType = rfl::TaggedUnion<
      "type_", DataFrameCmd, typename commands::ProjectCommand::AddDfFromJSONOp,
      typename commands::ProjectCommand::AddDfFromQueryOp,
      typename commands::ColumnCommand::SetFloatColumnUnitOp,
      typename commands::ColumnCommand::SetStringColumnUnitOp, FullTransformOp>;

  while (true) {
    const auto json_str = communication::Receiver::recv_string(_socket);

    const auto op = rfl::json::read<CmdType>(json_str);

    const auto handle =
        [&local_data_frame_manager, &local_column_manager,
         _socket](const auto& _op) -> std::optional<FullTransformOp> {
      using Type = std::decay_t<decltype(_op)>;
      if constexpr (std::is_same<Type, DataFrameCmd>()) {
        local_data_frame_manager.add_data_frame(rfl::get<"name_">(_op),
                                                _socket);
        return std::nullopt;
      } else if constexpr (std::is_same<Type,
                                        typename commands::ProjectCommand::
                                            AddDfFromJSONOp>()) {
        local_data_frame_manager.from_json(_op, _socket);
        return std::nullopt;
      } else if constexpr (std::is_same<Type,
                                        typename commands::ProjectCommand::
                                            AddDfFromQueryOp>()) {
        local_data_frame_manager.from_query(_op, _socket);
        return std::nullopt;
      } else if constexpr (std::is_same<Type, typename commands::ColumnCommand::
                                                  SetFloatColumnUnitOp>()) {
        local_column_manager.set_unit(_op, _socket);
        return std::nullopt;
      } else if constexpr (std::is_same<Type, typename commands::ColumnCommand::
                                                  SetStringColumnUnitOp>()) {
        local_column_manager.set_unit_categorical(_op, _socket);
        return std::nullopt;
      } else if constexpr (std::is_same<Type, FullTransformOp>()) {
        return _op;
      }
    };
    const auto cmd = rfl::visit(handle, op);
    if (cmd) {
      return *cmd;
    }
  }
}

}  // namespace handlers
}  // namespace engine
