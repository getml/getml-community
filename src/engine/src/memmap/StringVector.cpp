// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "memmap/StringVector.hpp"

namespace memmap {

  StringVector::StringVector(const std::shared_ptr<Pool> &_pool)
      : data_(Vector<char>(_pool)), indptr_(Vector<size_t>(_pool)) {
    indptr_.push_back(0);
  }

}  // namespace memmap
