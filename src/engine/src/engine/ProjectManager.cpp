// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/handlers/ProjectManager.hpp"

// ------------------------------------------------------------------------

#include <Poco/DirectoryIterator.h>

#include <stdexcept>

// ------------------------------------------------------------------------

#include "engine/pipelines/SaveParams.hpp"

// ------------------------------------------------------------------------

#include "engine/handlers/FileHandler.hpp"
#include "engine/handlers/PipelineManager.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace handlers {

void ProjectManager::add_data_frame_from_arrow(
    const std::string& _name, const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  data_frame_manager().from_arrow(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_csv(const std::string& _name,
                                             const Poco::JSON::Object& _cmd,
                                             Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  data_frame_manager().from_csv(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_db(const std::string& _name,
                                            const Poco::JSON::Object& _cmd,
                                            Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  params_.data_frame_manager_->from_db(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_json(
    const std::string& _name, const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  params_.data_frame_manager_->from_json(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_parquet(
    const std::string& _name, const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  data_frame_manager().from_parquet(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_query(
    const std::string& _name, const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  params_.data_frame_manager_->from_query(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_view(
    const std::string& _name, const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = JSON::get_value<bool>(_cmd, "append_");

  data_frame_manager().from_view(_name, _cmd, append, _socket);

  multithreading::ReadLock read_lock(params_.read_write_lock_);
}

// ------------------------------------------------------------------------

void ProjectManager::add_pipeline(const std::string& _name,
                                  const Poco::JSON::Object& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto pipeline = pipelines::Pipeline(_cmd);

  set_pipeline(_name, pipeline);

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::copy_pipeline(const std::string& _name,
                                   const Poco::JSON::Object& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const std::string other = JSON::get_value<std::string>(_cmd, "other_");

  const auto other_pipeline = get_pipeline(other);

  set_pipeline(_name, other_pipeline);

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

void ProjectManager::delete_data_frame(const std::string& _name,
                                       const Poco::JSON::Object& _cmd,
                                       Poco::Net::StreamSocket* _socket) {
  multithreading::WriteLock write_lock(params_.read_write_lock_);

  FileHandler::remove(_name, project_directory(), _cmd, &data_frames());

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::delete_pipeline(const std::string& _name,
                                     const Poco::JSON::Object& _cmd,
                                     Poco::Net::StreamSocket* _socket) {
  multithreading::WriteLock write_lock(params_.read_write_lock_);

  FileHandler::remove(_name, project_directory(), _cmd, &pipelines());

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::delete_project(const std::string& _name,
                                    Poco::Net::StreamSocket* _socket) {
  multithreading::WriteLock project_guard(params_.project_lock_);

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  if (_name == "") {
    throw std::runtime_error(
        "Project name can not be an "
        "empty string!");
  }

  Poco::File(params_.options_.all_projects_directory() + _name + "/")
      .remove(true);

  engine::communication::Sender::send_string("Success!", _socket);

  if (project_directory() ==
      params_.options_.all_projects_directory() + _name + "/") {
    exit(0);
  }
}

// ------------------------------------------------------------------------

void ProjectManager::list_data_frames(Poco::Net::StreamSocket* _socket) const {
  Poco::JSON::Object obj;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  Poco::JSON::Array in_memory;

  for (const auto& [key, value] : data_frames()) {
    in_memory.add(key);
  }

  Poco::JSON::Array on_disk;

  Poco::DirectoryIterator end;

  for (Poco::DirectoryIterator it(project_directory() + "data/"); it != end;
       ++it) {
    if (it->isDirectory()) {
      on_disk.add(it.name());
    }
  }

  read_lock.unlock();

  obj.set("in_memory", in_memory);

  obj.set("on_disk", on_disk);

  engine::communication::Sender::send_string("Success!", _socket);

  engine::communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::list_pipelines(Poco::Net::StreamSocket* _socket) const {
  Poco::JSON::Object obj;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  Poco::JSON::Array names;

  for (const auto& [key, value] : pipelines()) {
    names.add(key);
  }

  read_lock.unlock();

  obj.set("names", names);

  engine::communication::Sender::send_string("Success!", _socket);

  engine::communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::list_projects(Poco::Net::StreamSocket* _socket) const {
  Poco::JSON::Object obj;

  Poco::JSON::Array project_names;

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  Poco::DirectoryIterator end;

  for (Poco::DirectoryIterator it(params_.options_.all_projects_directory());
       it != end; ++it) {
    if (it->isDirectory()) {
      project_names.add(it.name());
    }
  }

  read_lock.unlock();

  obj.set("projects", project_names);

  engine::communication::Sender::send_string("Success!", _socket);

  engine::communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::load_data_container(const std::string& _name,
                                         Poco::Net::StreamSocket* _socket) {
  const auto path = project_directory() + "data_containers/" + _name + ".json";

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

void ProjectManager::load_data_frame(const std::string& _name,
                                     Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  // TODO
  auto df = FileHandler::load(data_frames(), params_.categories_.ptr(),
                              params_.join_keys_encoding_.ptr(),
                              params_.options_, _name);

  df.create_indices();

  weak_write_lock.upgrade();

  data_frames()[_name] = df;

  if (df.build_history()) {
    data_frame_tracker().add(df);
  }

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::load_pipeline(const std::string& _name,
                                   Poco::Net::StreamSocket* _socket) {
  const auto path = project_directory() + "pipelines/" + _name + "/";

  auto pipeline = pipelines::Load::load(path, params_.fe_tracker_.ptr(),
                                        params_.pred_tracker_.ptr(),
                                        params_.preprocessor_tracker_.ptr());

  set_pipeline(_name, pipeline);

  engine::communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::project_name(Poco::Net::StreamSocket* _socket) const {
  communication::Sender::send_string(params_.project_, _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_container(const std::string& _name,
                                         const Poco::JSON::Object& _cmd,
                                         Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto path = project_directory() + "data_containers/" + _name + ".json";

  const auto obj = jsonutils::JSON::get_object(_cmd, "container_");

  assert_true(obj);

  std::ofstream fs(path, std::ofstream::out);
  Poco::JSON::Stringifier::stringify(*obj, fs);
  fs.close();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_frame(const std::string& _name,
                                     Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(_name, &data_frames());

  df.save(params_.options_.temp_dir(), project_directory() + "data/", _name);

  FileHandler::save_encodings(project_directory(), params_.categories_.ptr(),
                              params_.join_keys_encoding_.ptr());

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ProjectManager::save_pipeline(const std::string& _name,
                                   Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pipeline = get_pipeline(_name);

  const auto fitted = pipeline.fitted();

  if (!fitted) {
    throw std::runtime_error(
        "The pipeline could not be saved. It has not been fitted.");
  }

  const auto path = project_directory() + "pipelines/";

  const auto params =
      pipelines::SaveParams{.categories_ = categories().strings(),
                            .fitted_ = *fitted,
                            .name_ = _name,
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

void ProjectManager::temp_dir(Poco::Net::StreamSocket* _socket) const {
  communication::Sender::send_string(params_.options_.temp_dir(), _socket);
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
