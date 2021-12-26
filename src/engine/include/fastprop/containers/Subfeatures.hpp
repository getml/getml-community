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
