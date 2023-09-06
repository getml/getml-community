// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/ProjectManager.hpp"
#include "rfl/always_false.hpp"

namespace engine {
namespace handlers {

void ProjectManager::execute_command(const Command& _command,
                                     Poco::Net::StreamSocket* _socket) {
  const auto handle = [this, _socket](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, typename Command::AddDfFromArrowOp>()) {
      add_data_frame_from_arrow(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::AddDfFromCSVOp>()) {
      add_data_frame_from_csv(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::AddDfFromDBOp>()) {
      add_data_frame_from_db(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::AddDfFromJSONOp>()) {
      add_data_frame_from_json(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::AddDfFromParquetOp>()) {
      add_data_frame_from_parquet(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::AddDfFromQueryOp>()) {
      add_data_frame_from_query(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::AddDfFromViewOp>()) {
      add_data_frame_from_view(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::PipelineOp>()) {
      add_pipeline(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::CopyPipelineOp>()) {
      copy_pipeline(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::DeleteDataFrameOp>()) {
      delete_data_frame(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::DeletePipelineOp>()) {
      delete_pipeline(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::DeleteProjectOp>()) {
      delete_project(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::ListDfsOp>()) {
      list_data_frames(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::ListPipelinesOp>()) {
      list_pipelines(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::ListProjectsOp>()) {
      list_projects(_cmd, _socket);
    } else if constexpr (std::is_same<
                             Type, typename Command::LoadDataContainerOp>()) {
      load_data_container(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::LoadDfOp>()) {
      load_data_frame(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::LoadPipelineOp>()) {
      load_pipeline(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::ProjectNameOp>()) {
      project_name(_cmd, _socket);
    } else if constexpr (std::is_same<
                             Type, typename Command::SaveDataContainerOp>()) {
      save_data_container(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::SaveDfOp>()) {
      save_data_frame(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::SavePipelineOp>()) {
      save_pipeline(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::TempDirOp>()) {
      temp_dir(_cmd, _socket);
    } else {
      static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  rfl::visit(handle, _command.val_);
}

}  // namespace handlers
}  // namespace engine
