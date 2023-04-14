// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/FileHandler.hpp"

#include "helpers/Endianness.hpp"

namespace engine {
namespace handlers {
// ------------------------------------------------------------------------

void FileHandler::create_project_directory(
    const std::string& _project_directory) {
  Poco::File(_project_directory).createDirectories();

  Poco::File(_project_directory + "data/").createDirectories();

  Poco::File(_project_directory + "data_containers/").createDirectories();

  Poco::File(_project_directory + "hyperopts/").createDirectories();

  Poco::File(_project_directory + "pipelines/").createDirectories();
}

// ------------------------------------------------------------------------

containers::DataFrame FileHandler::load(
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const config::Options& _options, const std::string& _name) {
  const auto path = _options.project_directory() + "data/" + _name + "/";

  Poco::File file(path);

  if (!file.exists()) {
    throw std::runtime_error("File or directory '" + path + "' not found!");
  }

  if (!file.isDirectory()) {
    throw std::runtime_error("'" + path + "' is not a directory!");
  }

  const auto pool = _options.make_pool();

  auto df =
      containers::DataFrame(_name, _categories, _join_keys_encoding, pool);

  df.load(path);

  return df;
}

// ------------------------------------------------------------------------

void FileHandler::load_encodings(const std::string& _path,
                                 containers::Encoding* _categories,
                                 containers::Encoding* _join_keys_encodings) {
  if (helpers::Endianness::is_little_endian()) {
    if (Poco::File(_path + "categories").exists()) {
      *_categories =
          FileHandler::read_strings_little_endian(_path + "categories");
    }

    if (Poco::File(_path + "join_keys_encoding").exists()) {
      *_join_keys_encodings =
          FileHandler::read_strings_little_endian(_path + "join_keys_encoding");
    }
  } else {
    if (Poco::File(_path + "categories").exists()) {
      *_categories = FileHandler::read_strings_big_endian(_path + "categories");
    }

    if (Poco::File(_path + "join_keys_encoding").exists()) {
      *_join_keys_encodings =
          FileHandler::read_strings_big_endian(_path + "join_keys_encoding");
    }
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> FileHandler::read_strings_big_endian(
    const std::string& _fname) {
  auto read_string = [](std::ifstream& _input) {
    std::string str;

    size_t str_size = 0;

    _input.read(reinterpret_cast<char*>(&str_size), sizeof(size_t));

    str.resize(str_size);

    _input.read(&str[0], str_size);

    return str;
  };

  std::ifstream input(_fname, std::ios::binary);

  auto strings = std::vector<std::string>(0);

  while (!input.eof()) {
    std::string str = read_string(input);

    if (!input.eof() || str != "") {
      strings.push_back(str);
    }
  }

  return strings;
}

// ----------------------------------------------------------------------------

std::vector<std::string> FileHandler::read_strings_little_endian(
    const std::string& _fname) {
  auto read_string = [](std::ifstream& _input) {
    std::string str;

    size_t str_size = 0;

    _input.read(reinterpret_cast<char*>(&str_size), sizeof(size_t));

    helpers::Endianness::reverse_byte_order(&str_size);

    str.resize(str_size);

    _input.read(&str[0], str_size);

    return str;
  };

  std::ifstream input(_fname, std::ios::binary);

  auto strings = std::vector<std::string>(0);

  while (!input.eof()) {
    std::string str = read_string(input);

    if (!input.eof() || str != "") {
      strings.push_back(str);
    }
  }

  return strings;
}

// ------------------------------------------------------------------------

void FileHandler::save_encodings(
    const std::string& _path,
    const std::shared_ptr<const containers::Encoding> _categories,
    const std::shared_ptr<const containers::Encoding> _join_keys_encodings) {
  if (helpers::Endianness::is_little_endian()) {
    if (_categories && _categories->size() > 0) {
      FileHandler::write_string_little_endian(_path + "categories",
                                              *_categories);
    }

    if (_join_keys_encodings && _join_keys_encodings->size() > 0) {
      FileHandler::write_string_little_endian(_path + "join_keys_encoding",
                                              *_join_keys_encodings);
    }
  } else {
    if (_categories && _categories->size() > 0) {
      FileHandler::write_string_big_endian(_path + "categories", *_categories);
    }

    if (_join_keys_encodings && _join_keys_encodings->size() > 0) {
      FileHandler::write_string_big_endian(_path + "join_keys_encoding",
                                           *_join_keys_encodings);
    }
  }
}

// ----------------------------------------------------------------------------

void FileHandler::write_string_big_endian(
    const std::string& _fname, const containers::Encoding& _strings) {
  std::ofstream output(_fname, std::ios::binary);

  auto write_string = [&output](const strings::String& _str) {
    size_t str_size = _str.size();

    output.write(reinterpret_cast<const char*>(&str_size), sizeof(size_t));

    output.write(_str.c_str(), _str.size());
  };

  for (size_t i = 0; i < _strings.size(); ++i) {
    write_string(_strings[i]);
  }
}

// ----------------------------------------------------------------------------

void FileHandler::write_string_little_endian(
    const std::string& _fname, const containers::Encoding& _strings) {
  std::ofstream output(_fname, std::ios::binary);

  auto write_string = [&output](const strings::String& _str) {
    size_t str_size = _str.size();

    helpers::Endianness::reverse_byte_order(&str_size);

    output.write(reinterpret_cast<const char*>(&str_size), sizeof(size_t));

    output.write(_str.c_str(), _str.size());
  };

  for (size_t i = 0; i < _strings.size(); ++i) {
    write_string(_strings[i]);
  }
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
