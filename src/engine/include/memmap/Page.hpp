// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MEMMAP_PAGE_HPP_
#define MEMMAP_PAGE_HPP_

#include <cstddef>

namespace memmap {
// ----------------------------------------------------------------------------

struct Page {
  Page();

  ~Page() = default;

  /// The size of the allocated memory block, in number of pages,
  /// if this is the first page of an allocated memory block, zero
  /// otherwise.
  size_t block_size_;

  /// Whether the page is allocated.
  bool is_allocated_;
};

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_PAGE_HPP_
