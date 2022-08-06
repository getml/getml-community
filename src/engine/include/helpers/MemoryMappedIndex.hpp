// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_MEMORYMAPPEDINDEX_HPP_
#define HELPERS_MEMORYMAPPEDINDEX_HPP_

// -------------------------------------------------------------------------

#include "memmap/memmap.hpp"

// -------------------------------------------------------------------------

#include "helpers/Int.hpp"

// -------------------------------------------------------------------------

namespace helpers {

typedef memmap::Index<Int> MemoryMappedIndex;

}  // namespace helpers

#endif  // HELPERS_MEMORYMAPPEDINDEX_HPP_

