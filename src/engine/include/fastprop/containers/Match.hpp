// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_CONTAINERS_MATCH_HPP_
#define FASTPROP_CONTAINERS_MATCH_HPP_

#include <cstddef>

namespace fastprop {
namespace containers {
// -------------------------------------------------------------------------

struct Match {
  size_t ix_input;

  size_t ix_output;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_MATCH_HPP_
