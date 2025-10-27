// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_CONTAINERS_SUBFEATURES_HPP_
#define FASTPROP_CONTAINERS_SUBFEATURES_HPP_

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/containers/ColumnView.hpp"

#include <map>

namespace fastprop {
namespace containers {

using Subfeatures = std::vector<ColumnView<Float, std::map<Int, Int>>>;

}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_SUBFEATURES_HPP_
