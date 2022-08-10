// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

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
