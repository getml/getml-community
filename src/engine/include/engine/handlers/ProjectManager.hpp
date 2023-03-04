// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_PROJECTMANAGER_HPP_
#define ENGINE_HANDLERS_PROJECTMANAGER_HPP_

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "commands/ProjectCommand.hpp"
#include "debug/debug.hpp"
#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/containers/containers.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/ProjectManagerParams.hpp"
#include "engine/pipelines/pipelines.hpp"

namespace engine {
namespace handlers {

class ProjectManager {
 public:
  typedef ProjectManagerParams::PipelineMapType PipelineMapType;
  typedef commands::ProjectCommand Command;

 public:
  ProjectManager(const ProjectManagerParams& _params) : params_(_params) {
    set_project(_params.project_);
  }

  ~ProjectManager() = default;

 public:
  /// Executes a command related to a database operation.
  template <class CommandType>
  void execute_command(const CommandType& _cmd,
                       Poco::Net::StreamSocket* _socket);

 private:
  /// Adds a new data frame read from an arrow table.
  void add_data_frame_from_arrow(const typename Command::AddDfFromArrowOp& _cmd,
                                 Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from one or several CSV files.
  void add_data_frame_from_csv(const typename Command::AddDfFromCSVOp& _cmd,
                               Poco::Net::StreamSocket* _socket);

  /// Adds a new data frame taken from the database.
  void add_data_frame_from_db(const typename Command::AddDfFromDBOp& _cmd,
                              Poco::Net::StreamSocket* _socket);

  /// Adds a new data frame taken parsed from a JSON.
  void add_data_frame_from_json(const typename Command::AddDfFromJSONOp& _cmd,
                                Poco::Net::StreamSocket* _socket);

  /// Adds a new data frame read from a parquet file.
  void add_data_frame_from_parquet(
      const typename Command::AddDfFromParquetOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Adds a new data frame generated from a query.
  void add_data_frame_from_query(const typename Command::AddDfFromQueryOp& _cmd,
                                 Poco::Net::StreamSocket* _socket);

  /// Adds a new data frame generated from a view.
  void add_data_frame_from_view(const typename Command::AddDfFromViewOp& _cmd,
                                Poco::Net::StreamSocket* _socket);

  /// Adds a new Pipeline to the project.
  void add_pipeline(const typename Command::PipelineOp& _cmd,
                    Poco::Net::StreamSocket* _socket);

  /// Duplicates a pipeline.
  void copy_pipeline(const typename Command::CopyPipelineOp& _cmd,
                     Poco::Net::StreamSocket* _socket);

  /// Deletes a data frame
  void delete_data_frame(const typename Command::DeleteDataFrameOp& _cmd,
                         Poco::Net::StreamSocket* _socket);

  /// Deletes a pipeline
  void delete_pipeline(const typename Command::DeletePipelineOp& _cmd,
                       Poco::Net::StreamSocket* _socket);

  /// Deletes a project
  void delete_project(const typename Command::DeleteProjectOp& _cmd,
                      Poco::Net::StreamSocket* _socket);

  /// Returns a list of all data_frames currently held in memory and held
  /// in the project directory.
  void list_data_frames(const typename Command::ListDfsOp& _cmd,
                        Poco::Net::StreamSocket* _socket) const;

  /// Returns a list of all pipelines currently held in memory.
  void list_pipelines(const typename Command::ListPipelinesOp& _cmd,
                      Poco::Net::StreamSocket* _socket) const;

  /// Returns a list of all projects.
  void list_projects(const typename Command::ListProjectsOp& _cmd,
                     Poco::Net::StreamSocket* _socket) const;

  /// Loads a data container.
  void load_data_container(const typename Command::LoadDataContainerOp& _cmd,
                           Poco::Net::StreamSocket* _socket);

  /// Loads a data frame
  void load_data_frame(const typename Command::LoadDfOp& _cmd,
                       Poco::Net::StreamSocket* _socket);

  /// Loads a pipeline
  void load_pipeline(const typename Command::LoadPipelineOp& _cmd,
                     Poco::Net::StreamSocket* _socket);

  /// Get the name of the current project.
  void project_name(const typename Command::ProjectNameOp& _cmd,
                    Poco::Net::StreamSocket* _socket) const;

  /// Saves a data container to disk.
  void save_data_container(const typename Command::SaveDataContainerOp& _cmd,
                           Poco::Net::StreamSocket* _socket);

  /// Saves a data frame
  void save_data_frame(const typename Command::SaveDfOp& _cmd,
                       Poco::Net::StreamSocket* _socket);

  /// Saves a pipeline to disc.
  void save_pipeline(const typename Command::SavePipelineOp& _cmd,
                     Poco::Net::StreamSocket* _socket);

  /// Sets the current project
  void set_project(const std::string& _name);

  /// Get the path of the directory for tempfiles.
  void temp_dir(const typename Command::TempDirOp& _cmd,
                Poco::Net::StreamSocket* _socket) const;

 public:
  /// Trivial accessor
  std::string project_directory() const {
    return params_.options_.project_directory();
  }

 private:
  /// Deletes all pipelines and data frames (from memory only) and clears all
  /// encodings.
  void clear();

  /// Loads a JSON object from a file.
  Poco::JSON::Object load_json_obj(const std::string& _fname) const;

 private:
  /// Trivial accessor
  containers::Encoding& categories() { return *params_.categories_; }

  /// Trivial accessor
  const containers::Encoding& categories() const {
    return *params_.categories_;
  }

  /// Trivial accessor
  std::map<std::string, containers::DataFrame>& data_frames() {
    return *params_.data_frames_;
  }

  /// Trivial (const) accessor
  const std::map<std::string, containers::DataFrame>& data_frames() const {
    return *params_.data_frames_;
  }

  /// Trivial accessor
  dependency::DataFrameTracker& data_frame_tracker() {
    return *params_.data_frame_tracker_;
  }

  /// Trivial accessor
  DataFrameManager& data_frame_manager() {
    return *params_.data_frame_manager_;
  }

  /// Trivial (const) accessor
  const DataFrameManager& data_frame_manager() const {
    return *params_.data_frame_manager_;
  }

  /// Trivial accessor
  dependency::FETracker& fe_tracker() { return *params_.fe_tracker_; }

  /// Returns a deep copy of a pipeline.
  pipelines::Pipeline get_pipeline(const std::string& _name) const {
    multithreading::ReadLock read_lock(params_.read_write_lock_);
    auto p = utils::Getter::get(_name, pipelines());
    return p;
  }

  /// Trivial accessor
  containers::Encoding& join_keys_encoding() {
    return *params_.join_keys_encoding_;
  }

  /// Trivial accessor
  const containers::Encoding& join_keys_encoding() const {
    return *params_.join_keys_encoding_;
  }

  /// Trivial (private) accessor
  const communication::Logger& logger() { return *params_.logger_; }

  /// Trivial (private) accessor
  const communication::Monitor& monitor() const { return *params_.monitor_; }

  /// Trivial (private) accessor
  PipelineMapType& pipelines() { return *params_.pipelines_; }

  /// Trivial (const private) accessor
  const PipelineMapType& pipelines() const { return *params_.pipelines_; }

  /// Trivial accessor
  dependency::PredTracker& pred_tracker() { return *params_.pred_tracker_; }

  /// Trivial (private) setter.
  void set_pipeline(const std::string& _name,
                    const pipelines::Pipeline& _pipeline) {
    multithreading::WriteLock write_lock(params_.read_write_lock_);
    pipelines().insert_or_assign(_name, _pipeline);
  }

 private:
  /// The underlying parameters.
  const ProjectManagerParams params_;
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class CommandType>
void ProjectManager::execute_command(const CommandType& _cmd,
                                     Poco::Net::StreamSocket* _socket) {
  using Type = std::decay_t<CommandType>;

  if constexpr (std::is_same<Type, typename Command::AddDfFromArrowOp>()) {
    add_data_frame_from_arrow(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::AddDfFromCSVOp>()) {
    add_data_frame_from_csv(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::AddDfFromDBOp>()) {
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
  } else if constexpr (std::is_same<Type, typename Command::CopyPipelineOp>()) {
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
  } else if constexpr (std::is_same<Type, typename Command::ListProjectsOp>()) {
    list_projects(_cmd, _socket);
  } else if constexpr (std::is_same<Type,
                                    typename Command::LoadDataContainerOp>()) {
    load_data_container(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::LoadDfOp>()) {
    load_data_frame(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::LoadPipelineOp>()) {
    load_pipeline(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::ProjectNameOp>()) {
    project_name(_cmd, _socket);
  } else if constexpr (std::is_same<Type,
                                    typename Command::SaveDataContainerOp>()) {
    save_data_container(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::SaveDfOp>()) {
    save_data_frame(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::SavePipelineOp>()) {
    save_pipeline(_cmd, _socket);
  } else if constexpr (std::is_same<Type, typename Command::TempDirOp>()) {
    temp_dir(_cmd, _socket);
  } else {
    []<bool _flag = false>() {
      static_assert(_flag, "Not all cases were covered.");
    }
    ();
  }
}

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PROJECTMANAGER_HPP_
