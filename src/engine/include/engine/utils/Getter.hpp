// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_UTILS_GETTER_HPP_
#define ENGINE_UTILS_GETTER_HPP_

// ------------------------------------------------------------------------

#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// ------------------------------------------------------------------------

namespace engine {
namespace utils {

struct Getter {
  /// Gets object _name from map
  template <class T>
  static T &get(const std::string &_name, std::map<std::string, T> *_map);

  /// Gets object _name from map
  template <class T>
  static T get(const std::string &_name, const std::map<std::string, T> &_map);

  /// Gets vector of objects with _names from map
  template <class T>
  static std::vector<T> get(const std::vector<std::string> &_names,
                            std::map<std::string, T> *&_map);

  /// Gets object _name from map, if it exists
  /// and returns a boolean indicating whether it does.
  template <class T>
  static std::pair<T *, bool> get_if_exists(const std::string &_name,
                                            std::map<std::string, T> *_map);
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class T>
T &Getter::get(const std::string &_name, std::map<std::string, T> *_map) {
  auto it = _map->find(_name);

  if (it == _map->end()) {
    std::string warning_message = "'";
    warning_message.append(_name);
    warning_message.append("' not found. ");

    throw std::runtime_error(warning_message);
  }

  return it->second;
}

// ------------------------------------------------------------------------

template <class T>
T Getter::get(const std::string &_name, const std::map<std::string, T> &_map) {
  auto it = _map.find(_name);

  if (it == _map.end()) {
    std::string warning_message = "'";
    warning_message.append(_name);
    warning_message.append("' not found. ");

    throw std::runtime_error(warning_message);
  }

  return it->second;
}

// ------------------------------------------------------------------------

template <class T>
std::vector<T> Getter::get(const std::vector<std::string> &_names,
                           std::map<std::string, T> *&_map) {
  std::vector<T> vec;

  for (auto &name : _names) {
    auto elem = get(_map, name);

    vec.push_back(elem);
  }

  return vec;
}

// ------------------------------------------------------------------------

template <class T>
std::pair<T *, bool> Getter::get_if_exists(const std::string &_name,
                                           std::map<std::string, T> *_map) {
  auto it = _map->find(_name);

  if (it == _map->end()) {
    return std::make_pair(nullptr, false);
  }

  return std::make_pair(&it->second, true);
}

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_GETTER_HPP_
