#ifndef MULTIREL_CONTAINERS_MATCH_HPP_
#define MULTIREL_CONTAINERS_MATCH_HPP_

// ----------------------------------------------------------------------------

#include <cstddef>

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace containers {
// ----------------------------------------------------------------------------

struct Match {
  bool activated;

  Int categorical_value;

  size_t ix_x_perip;

  size_t ix_x_popul;

  Float numerical_value;
};

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINERS_MATCH_HPP_
