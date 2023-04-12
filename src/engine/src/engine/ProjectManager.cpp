// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/ProjectManager.hpp"

#include <Poco/DirectoryIterator.h>

#include <stdexcept>

#include "engine/handlers/FileHandler.hpp"
#include "engine/handlers/PipelineManager.hpp"
#include "engine/pipelines/SaveParams.hpp"
#include "fct/always_false.hpp"
#include "fct/make_named_tuple.hpp"

namespace engine {
namespace handlers {

void ProjectManager::add_data_frame_from_arrow(
    const typename Command::AddDfFromArrowOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  data_frame_manager().from_arrow(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_csv(
    const typename Command::AddDfFromCSVOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  data_frame_manager().from_csv(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_db(
    const typename Command::AddDfFromDBOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  params_.data_frame_manager_->from_db(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_json(
    const typename Command::AddDfFromJSONOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  params_.data_frame_manager_->from_json(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_parquet(
    const typename Command::AddDfFromParquetOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  data_frame_manager().from_parquet(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_query(
    const typename Command::AddDfFromQueryOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  params_.data_frame_manager_->from_query(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_view(
    const typename Command::AddDfFromViewOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  data_frame_manager().from_view(_cmd, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::add_pipeline(const typename Command::PipelineOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto pipeline = pipelines::Pipeline(_cmd);

  set_pipeline(_cmd.get<"name_">(), pipeline);

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::copy_pipeline(const typename Command::CopyPipelineOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto& other = _cmd.get<"other_">();

  const auto& name = _cmd.get<"name_">();

  const auto other_pipeline = get_pipeline(other);

  set_pipeline(name, other_pipeline);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::clear() {
  data_frames() = std::map<std::string, engine::containers::DataFrame>();

  pipelines() = engine::handlers::PipelineManager::PipelineMapType();

  categories().clear();

  join_keys_encoding().clear();

  data_frame_tracker().clear();

  fe_tracker().clear();

  pred_tracker().clear();
}

// ------------------------------------------------------------------------

void ProjectManager::delete_data_frame(
    const typename Command::DeleteDataFrameOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  FileHandler::remove(name, project_directory(), _cmd.get<"mem_only_">(),
                      &data_frames());

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::delete_pipeline(
    const typename Command::DeletePipelineOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  FileHandler::remove(name, project_directory(), _cmd.get<"mem_only_">(),
                      &pipelines());

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::delete_project(
    const typename Command::DeleteProjectOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WriteLock project_guard(params_.project_lock_);

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  if (name == "") {
    throw std::runtime_error(
        "Project name can not be an "
        "empty string!");
  }

  Poco::File(params_.options_.all_projects_directory() + name + "/")
      .remove(true);

  engine::communication::Sender::send_string("Success!", _socket);

  if (project_directory() ==
      params_.options_.all_projects_directory() + name + "/") {
    exit(0);
  }
}

// ------------------------------------------------------------------------

void ProjectManager::list_data_frames(const typename Command::ListDfsOp& _cmd,
                                      Poco::Net::StreamSocket* _socket) const {
  multithreading::ReadLock read_lock(params_.read_write_lock_);

  std::vector<std::string> in_memory;

  for (const auto& [key, value] : data_frames()) {
    in_memory.push_back(key);
  }

  std::vector<std::string> on_disk;

  Poco::DirectoryIterator end;

  for (Poco::DirectoryIterator it(project_directory() + "data/"); it != end;
       ++it) {
    if (it->isDirectory()) {
      on_disk.push_back(it.name());
    }
  }

  read_lock.unlock();

  const auto obj = fct::make_field<"in_memory">(in_memory) *
                   fct::make_field<"on_disk">(on_disk);

  engine::communication::Sender::send_string("Success!", _socket);

  engine::communication::Sender::send_string(json::to_json(obj), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::list_pipelines(
    const typename Command::ListPipelinesOp& _cmd,
    Poco::Net::StreamSocket* _socket) const {
  multithreading::ReadLock read_lock(params_.read_write_lock_);

  std::vector<std::string> names;

  for (const auto& [key, value] : pipelines()) {
    names.push_back(key);
  }

  read_lock.unlock();

  const auto obj = fct::make_named_tuple(fct::make_field<"names">(names));

  engine::communication::Sender::send_string("Success!", _socket);

  engine::communication::Sender::send_string(json::to_json(obj), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::list_projects(const typename Command::ListProjectsOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) const {
  std::vector<std::string> project_names;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  Poco::DirectoryIterator end;

  for (Poco::DirectoryIterator it(params_.options_.all_projects_directory());
       it != end; ++it) {
    if (it->isDirectory()) {
      project_names.push_back(it.name());
    }
  }

  read_lock.unlock();

  const auto obj =
      fct::make_named_tuple(fct::make_field<"projects">(project_names));

  engine::communication::Sender::send_string("Success!", _socket);

  engine::communication::Sender::send_string(json::to_json(obj), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::load_data_container(
    const typename Command::LoadDataContainerOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto path = project_directory() + "data_containers/" + name + ".json";

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  std::ifstream input(path);

  if (!input.is_open()) {
    throw std::runtime_error("File '" + path + "' not found!");
  }

  std::stringstream json;

  std::string line;

  while (std::getline(input, line)) {
    json << line;
  }

  input.close();

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json.str(), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::load_data_frame(const typename Command::LoadDfOp& _cmd,
                                     Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  auto df = FileHandler::load(data_frames(), params_.categories_.ptr(),
                              params_.join_keys_encoding_.ptr(),
                              params_.options_, name);

  df.create_indices();

  weak_write_lock.upgrade();

  data_frames()[name] = df;

  if (df.build_history()) {
    data_frame_tracker().add(df, *df.build_history());
  }

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::load_pipeline(const typename Command::LoadPipelineOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto path = project_directory() + "pipelines/" + name + "/";

  const auto pipeline = pipelines::Load::load(
      path, params_.fe_tracker_.ptr(), params_.pred_tracker_.ptr(),
      params_.preprocessor_tracker_.ptr());

  set_pipeline(name, pipeline);

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::project_name(const typename Command::ProjectNameOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) const {
  communication::Sender::send_string(params_.project_, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_container(
    const typename Command::SaveDataContainerOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto path = project_directory() + "data_containers/" + name + ".json";

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  // TODO: Define data container
  /*const auto obj = jsonutils::JSON::get_object(_cmd, "container_");

  assert_true(obj);

  std::ofstream fs(path, std::ofstream::out);
  Poco::JSON::Stringifier::stringify(*obj, fs);
  fs.close();*/

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_frame(const typename Command::SaveDfOp& _cmd,
                                     Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(name, &data_frames());

  df.save(params_.options_.temp_dir(), project_directory() + "data/", name);

  FileHandler::save_encodings(project_directory(), params_.categories_.ptr(),
                              params_.join_keys_encoding_.ptr());

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::save_pipeline(const typename Command::SavePipelineOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pipeline = get_pipeline(name);

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error(
        "The pipeline could not be saved. It has not been fitted.");
  }

  const auto path = project_directory() + "pipelines/";

  const auto params =
      pipelines::SaveParams{.categories_ = categories().strings(),
                            .fitted_ = *fitted,
                            .name_ = name,
                            .path_ = path,
                            .pipeline_ = pipeline,
                            .temp_dir_ = params_.options_.temp_dir()};

  pipelines::Save::save(params);

  FileHandler::save_encodings(project_directory(), params_.categories_.ptr(),
                              nullptr);

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::set_project(const std::string& _project) {
  if (_project == "") {
    throw std::runtime_error("Project name can not be an empty string!");
  }

  handlers::FileHandler::create_project_directory(project_directory());

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  clear();

  FileHandler::load_encodings(project_directory(), &categories(),
                              &join_keys_encoding());

  write_lock.unlock();
}

// ------------------------------------------------------------------------

void ProjectManager::temp_dir(const typename Command::TempDirOp& _cmd,
                              Poco::Net::StreamSocket* _socket) const {
  communication::Sender::send_string(params_.options_.temp_dir(), _socket);
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
