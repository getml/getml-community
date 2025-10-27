// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "multithreading/ReadWriteLock.hpp"

namespace multithreading {

ReadWriteLock::ReadWriteLock()
    : active_weak_writer_exists_(false),
      active_writer_exists_(false),
      num_active_readers_(0),
      num_waiting_weak_writers_(0),
      num_waiting_writers_(0) {
  assert_true(no_active_writers());
}

}  // namespace multithreading
