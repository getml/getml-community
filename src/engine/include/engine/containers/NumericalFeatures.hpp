#ifndef ENGINE_CONTAINERS_NUMERICALFEATURES_HPP_
#define ENGINE_CONTAINERS_NUMERICALFEATURES_HPP_

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "engine/Float.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace containers {

typedef std::vector<helpers::Feature<Float>> NumericalFeatures;

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_NUMERICALFEATURES_HPP_
