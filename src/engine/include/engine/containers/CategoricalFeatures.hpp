#ifndef ENGINE_CONTAINERS_CATEGORICALFEATURES_HPP_
#define ENGINE_CONTAINERS_CATEGORICALFEATURES_HPP_

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "engine/Int.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace containers {
// ----------------------------------------------------------------------------

typedef std::vector<helpers::Feature<Int>> CategoricalFeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_CATEGORICALFEATURES_HPP_
