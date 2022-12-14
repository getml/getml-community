// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_CONFIG_ENGINEOPTIONS_
#define ENGINE_CONFIG_ENGINEOPTIONS_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <string>

// ----------------------------------------------------------------------------

#include "engine/JSON.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace config {

// ----------------------------------------------------------------------------
// Configuration information for the engine

struct EngineOptions {
  static constexpr bool IN_MEMORY = true;
  static constexpr bool MEMORY_MAPPING = false;

  // ------------------------------------------------------

 public:
  EngineOptions(const Poco::JSON::Object& _json_obj)
      : in_memory_(IN_MEMORY),
        port_(JSON::get_value<size_t>(_json_obj, "port")) {}

  EngineOptions() : port_(1708) {}

  ~EngineOptions() = default;

  // ------------------------------------------------------

  /// Trivial accessor
  const size_t port() const { return port_; }

  // ------------------------------------------------------

  /// Whether you want this to be in memory or memory mapped.
  bool in_memory_;

  /// The port of the engine
  size_t port_;

  /// The name of the project adressed by this engine
  std::string project_;

  // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_ENGINEOPTIONS_
