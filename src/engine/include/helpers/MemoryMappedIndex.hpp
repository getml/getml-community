// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_MEMORYMAPPEDINDEX_HPP_
#define HELPERS_MEMORYMAPPEDINDEX_HPP_

#include "helpers/Int.hpp"
#include "memmap/Index.hpp"

namespace helpers {

using MemoryMappedIndex = memmap::Index<Int>;

}  // namespace helpers

#endif  // HELPERS_MEMORYMAPPEDINDEX_HPP_
