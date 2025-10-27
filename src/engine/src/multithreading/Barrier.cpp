// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "multithreading/Barrier.hpp"

namespace multithreading {

Barrier::Barrier(size_t _num_threads)
    : generation_(0),
      num_threads_(_num_threads),
      num_threads_left_(_num_threads) {}

}  // namespace multithreading
