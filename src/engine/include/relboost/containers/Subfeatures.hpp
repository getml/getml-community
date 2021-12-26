#ifndef RELBOOST_CONTAINERS_SUBFEATURES_HPP_
#define RELBOOST_CONTAINERS_SUBFEATURES_HPP_

// ----------------------------------------------------------------------------

#include <map>
#include <vector>

// ----------------------------------------------------------------------------

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"

// ----------------------------------------------------------------------------

#include "relboost/containers/ColumnView.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace containers {
// ----------------------------------------------------------------------------

typedef std::vector<ColumnView<Float, std::map<Int, Int>>> Subfeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_SUBFEATURES_HPP_
