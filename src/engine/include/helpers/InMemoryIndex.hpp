// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_INMEMORYINDEX_HPP_
#define HELPERS_INMEMORYINDEX_HPP_

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "helpers/Int.hpp"

namespace helpers {

typedef std::unordered_map<Int, std::vector<size_t>> InMemoryIndex;

}  // namespace helpers

#endif  // HELPERS_INMEMORYINDEX_HPP_

