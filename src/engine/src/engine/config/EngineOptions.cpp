// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/config/EngineOptions.hpp"

namespace engine::config {

EngineOptions::EngineOptions(const ReflectionType& _obj)
    : in_memory_(IN_MEMORY), port_(_obj.get<"port">()) {}

EngineOptions::EngineOptions() : port_(1708) {}

}  // namespace engine::config
