// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MEMMAP_FREEBLOCK_HPP_
#define MEMMAP_FREEBLOCK_HPP_

#include <cstddef>

namespace memmap {

struct FreeBlock {
  /// The page number marking the beginnung of the free block
  size_t begin_;

  /// The page number marking the end of the free block.
  size_t end_;
};

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_FREEBLOCK_HPP_
