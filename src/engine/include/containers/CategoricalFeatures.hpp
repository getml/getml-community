// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_CATEGORICALFEATURES_HPP_
#define CONTAINERS_CATEGORICALFEATURES_HPP_

#include "containers/Int.hpp"
#include "helpers/Feature.hpp"

namespace containers {

using CategoricalFeatures = std::vector<helpers::Feature<Int>>;

}  // namespace containers

#endif  // CONTAINERS_CATEGORICALFEATURES_HPP_
