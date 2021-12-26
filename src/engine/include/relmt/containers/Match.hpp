#ifndef RELMT_CONTAINERS_MATCH_HPP_
#define RELMT_CONTAINERS_MATCH_HPP_

#include <cstdint>

#include "helpers/helpers.hpp"

namespace relmt {
namespace containers {
// -------------------------------------------------------------------------

struct Match {
  size_t ix_input;

  size_t ix_output;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relmt

#endif  // RELMT_CONTAINERS_MATCH_HPP_
