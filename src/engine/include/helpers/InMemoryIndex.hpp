// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_INMEMORYINDEX_HPP_
#define HELPERS_INMEMORYINDEX_HPP_

#include "helpers/Int.hpp"

#include <cstddef>
#include <unordered_map>
#include <vector>

namespace helpers {

using InMemoryIndex = std::unordered_map<Int, std::vector<size_t>>;

}  // namespace helpers

#endif  // HELPERS_INMEMORYINDEX_HPP_
