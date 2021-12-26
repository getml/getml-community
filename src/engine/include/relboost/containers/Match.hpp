#ifndef RELBOOST_CONTAINERS_MATCH_HPP_
#define RELBOOST_CONTAINERS_MATCH_HPP_

#include <cstdint>

#include "helpers/helpers.hpp"

namespace relboost {
namespace containers {
// -------------------------------------------------------------------------

struct Match {
  size_t ix_input;
  size_t ix_output;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_MATCH_HPP_
