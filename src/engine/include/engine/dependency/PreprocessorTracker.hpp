#ifndef ENGINE_DEPENDENCY_PREPROCESSORTRACKER_HPP_
#define ENGINE_DEPENDENCY_PREPROCESSORTRACKER_HPP_

// -------------------------------------------------------------------------

#include "engine/preprocessors/preprocessors.hpp"

// -------------------------------------------------------------------------

#include "engine/dependency/Tracker.hpp"

// -------------------------------------------------------------------------

namespace engine {
namespace dependency {

typedef Tracker<preprocessors::Preprocessor> PreprocessorTracker;

}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_PREPROCESSORTRACKER_HPP_
