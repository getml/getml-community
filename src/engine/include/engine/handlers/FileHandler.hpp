// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_FILEHANDLER_HPP_
#define ENGINE_HANDLERS_FILEHANDLER_HPP_

#include <Poco/File.h>
#include <Poco/Path.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "containers/containers.hpp"
#include "engine/config/config.hpp"
#include "engine/pipelines/pipelines.hpp"
#include "engine/utils/Getter.hpp"

namespace engine {
namespace handlers {

struct FileHandler {
  /// Creates a project directory, including the subfolders
  /// "data" and "models", if they do not already exist
  static void create_project_directory(const std::string& _project_directory);

  /// Determines the appropriate file ending for ColumnType
  template <class ColumnType>
  static std::string file_ending();

  /// Loads matrix from disc
  template <class ColumnType>
  static ColumnType load(const std::string& _name,
                         Poco::Net::StreamSocket* _socket);

  /// Loads data frame from disc
  static containers::DataFrame load(
      const std::map<std::string, containers::DataFrame>& _data_frames,
      const std::shared_ptr<containers::Encoding>& _categories,
      const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
      const config::Options& _options, const std::string& _name);

  /// Load the encodings
  static void load_encodings(const std::string& _path,
                             containers::Encoding* _categories,
                             containers::Encoding* _join_keys_encodings);

  /// Reads categories or join keys encoding from file
  static std::vector<std::string> read_strings_big_endian(
      const std::string& _fname);

  /// Reads categories or join keys encoding from file
  static std::vector<std::string> read_strings_little_endian(
      const std::string& _fname);

  /// Removes DataFrame or Model
  template <class Type>
  static void remove(const std::string& _name,
                     const std::string& _project_directory,
                     const bool _mem_only, std::map<std::string, Type>* _map);

  /// Saves matrix to disc
  template <class ColumnType>
  static void save(const std::string& _name,
                   const std::map<std::string, ColumnType>& _map,
                   Poco::Net::StreamSocket* _socket);

  /// Saves the encodings to disk
  static void save_encodings(
      const std::string& _path,
      const std::shared_ptr<const containers::Encoding> _categories,
      const std::shared_ptr<const containers::Encoding> _join_keys_encodings);

  /// Writes categories or join keys encoding to file
  static void write_string_big_endian(const std::string& _fname,
                                      const containers::Encoding& _strings);

  /// Writes categories or join keys encoding to file
  static void write_string_little_endian(const std::string& _fname,
                                         const containers::Encoding& _strings);

  // ------------------------------------------------------------------------

  /// Deletes the temporary directory
  static void delete_temp_dir(const std::string& _path) {
    auto file = Poco::File(_path);
    file.createDirectories();
    file.remove(true);
  }

  /// Makes the filename for a data frame.
  template <
      typename RType,
      typename std::enable_if<std::is_same<RType, containers::DataFrame>::value,
                              int>::type = 0>
  static std::string make_fname(const std::string& _project_directory,
                                const std::string& _name) {
    return _project_directory + "data/" + _name;
  }

  /// Makes the filename for a pipeline.
  template <typename RType,
            typename std::enable_if<
                std::is_same<RType, pipelines::Pipeline>::value, int>::type = 0>
  static std::string make_fname(const std::string& _project_directory,
                                const std::string& _name) {
    return _project_directory + "pipelines/" + _name;
  }

  // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class ColumnType>
std::string FileHandler::file_ending() {
  if (std::is_same<ColumnType, containers::Column<Float>>::value) {
    return ".mat";
  } else if (std::is_same<ColumnType, containers::Column<Int>>::value) {
    return ".key";
  } else {
    return "";
  }
}

// ------------------------------------------------------------------------

template <class ColumnType>
ColumnType FileHandler::load(const std::string& _name,
                             Poco::Net::StreamSocket* _socket) {
  std::string fname = _name + FileHandler::file_ending<ColumnType>();

  if (!Poco::File(fname).exists()) {
    throw std::runtime_error("File '" +
                             Poco::Path(fname).makeAbsolute().toString() +
                             "' not found!");
  }

  communication::Sender::send_string("Found!", _socket);

  ColumnType mat;

  mat.load(fname);

  return mat;
}

// ------------------------------------------------------------------------

template <class Type>
void FileHandler::remove(const std::string& _name,
                         const std::string& _project_directory,
                         const bool _mem_only,
                         std::map<std::string, Type>* _map) {
  _map->erase(_name);

  if (!_mem_only && _project_directory != "") {
    const auto fname = make_fname<Type>(_project_directory, _name);

    auto file = Poco::File(fname);

    if (file.exists()) {
      file.remove(true);
    }
  }
}

// ------------------------------------------------------------------------

template <class ColumnType>
void FileHandler::save(const std::string& _name,
                       const std::map<std::string, ColumnType>& _map,
                       Poco::Net::StreamSocket* _socket)

{
  auto mat = utils::Getter::get(_map, _name);

  communication::Sender::send_string("Found!", _socket);

  std::string fname = _name;

  fname.append(FileHandler::file_ending<ColumnType>());

  mat.save(fname);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_FILEHANDLER_HPP_
