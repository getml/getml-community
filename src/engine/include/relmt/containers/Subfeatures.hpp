#ifndef RELMT_CONTAINERS_SUBFEATURES_HPP_
#define RELMT_CONTAINERS_SUBFEATURES_HPP_

// ----------------------------------------------------------------------------

#include <map>
#include <vector>

// ----------------------------------------------------------------------------

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"

// ----------------------------------------------------------------------------

#include "relboost/containers/ColumnView.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace containers {
// ----------------------------------------------------------------------------

typedef std::vector<ColumnView<Float, std::map<Int, Int>>> Subfeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace relmt

#endif  // RELMT_CONTAINERS_SUBFEATURES_HPP_
