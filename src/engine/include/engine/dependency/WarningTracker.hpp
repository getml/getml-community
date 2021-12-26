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
