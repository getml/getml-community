// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "multithreading/Communicator.hpp"

namespace multithreading {

Communicator::Communicator(size_t _num_threads)
    : barrier_(_num_threads),
      checkpoint_(true),
      main_thread_id_(std::this_thread::get_id()),
      num_threads_(_num_threads),
      num_threads_left_(_num_threads) {}

}  // namespace multithreading
