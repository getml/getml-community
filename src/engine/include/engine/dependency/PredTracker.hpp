// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_DEPENDENCY_PREDTRACKER_HPP_
#define ENGINE_DEPENDENCY_PREDTRACKER_HPP_

#include "engine/dependency/Tracker.hpp"
#include "predictors/predictors.hpp"

namespace engine {
namespace dependency {

typedef Tracker<predictors::Predictor> PredTracker;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_PREDTRACKER_HPP_
