// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_INDEX_HPP_
#define HELPERS_INDEX_HPP_

// -------------------------------------------------------------------------

#include <variant>

// -------------------------------------------------------------------------

#include "helpers/InMemoryIndex.hpp"
#include "helpers/MemoryMappedIndex.hpp"

// -------------------------------------------------------------------------

namespace helpers {
// -------------------------------------------------------------------------

typedef std::variant<InMemoryIndex, MemoryMappedIndex> Index;

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_INDEX_HPP_
