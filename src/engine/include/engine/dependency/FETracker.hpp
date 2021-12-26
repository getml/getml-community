#ifndef ENGINE_DEPENDENCY_FETRACKER_HPP_
#define ENGINE_DEPENDENCY_FETRACKER_HPP_

// -------------------------------------------------------------------------

#include "engine/featurelearners/featurelearners.hpp"

// -------------------------------------------------------------------------

#include "engine/dependency/Tracker.hpp"

// -------------------------------------------------------------------------

namespace engine {
namespace dependency {

typedef Tracker<featurelearners::AbstractFeatureLearner> FETracker;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_FETRACKER_HPP_
