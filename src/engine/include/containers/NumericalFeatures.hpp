// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_NUMERICALFEATURES_HPP_
#define CONTAINERS_NUMERICALFEATURES_HPP_

#include "containers/Float.hpp"
#include "helpers/Feature.hpp"

#include <vector>

namespace containers {

using NumericalFeatures = std::vector<helpers::Feature<Float>>;

}  // namespace containers

#endif  // CONTAINERS_NUMERICALFEATURES_HPP_
