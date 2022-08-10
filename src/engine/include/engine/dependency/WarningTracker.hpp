// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_DEPENDENCY_WARNINGTRACKER_HPP_
#define ENGINE_DEPENDENCY_WARNINGTRACKER_HPP_

// -------------------------------------------------------------------------

#include "engine/communication/communication.hpp"

// -------------------------------------------------------------------------

#include "engine/dependency/Tracker.hpp"

// -------------------------------------------------------------------------

namespace engine {
namespace dependency {

typedef Tracker<communication::Warnings> WarningTracker;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_WARNINGTRACKER_HPP_
