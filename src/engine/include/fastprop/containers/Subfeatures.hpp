// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FASTPROP_CONTAINERS_SUBFEATURES_HPP_
#define FASTPROP_CONTAINERS_SUBFEATURES_HPP_

// ----------------------------------------------------------------------------

#include <map>

// ----------------------------------------------------------------------------

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "fastprop/containers/ColumnView.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace containers {
// ----------------------------------------------------------------------------

typedef std::vector<ColumnView<Float, std::map<Int, Int>>> Subfeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_SUBFEATURES_HPP_
