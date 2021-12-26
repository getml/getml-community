#ifndef ENGINE_DEPENDENCY_PREDTRACKER_HPP_
#define ENGINE_DEPENDENCY_PREDTRACKER_HPP_

// -------------------------------------------------------------------------

#include "predictors/predictors.hpp"

// -------------------------------------------------------------------------

#include "engine/dependency/Tracker.hpp"

// -------------------------------------------------------------------------

namespace engine {
namespace dependency {

typedef Tracker<predictors::Predictor> PredTracker;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_PREDTRACKER_HPP_
