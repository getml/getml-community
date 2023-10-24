// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_NUMERICALFEATURES_HPP_
#define CONTAINERS_NUMERICALFEATURES_HPP_

#include <vector>

#include "containers/Float.hpp"
#include "helpers/Feature.hpp"

namespace containers {

typedef std::vector<helpers::Feature<Float>> NumericalFeatures;

}  // namespace containers

#endif  // CONTAINERS_NUMERICALFEATURES_HPP_
