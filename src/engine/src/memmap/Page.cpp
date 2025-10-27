// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "memmap/Page.hpp"

namespace memmap {

Page::Page() : block_size_(0), is_allocated_(false) {}

}  // namespace memmap
