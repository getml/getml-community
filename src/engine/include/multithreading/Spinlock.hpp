// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef MULTITHREADING_SPINLOCK_HPP_
#define MULTITHREADING_SPINLOCK_HPP_

// ----------------------------------------------------------------------------

#include <thread>

// ----------------------------------------------------------------------------

namespace multithreading {
// ----------------------------------------------------------------------------
class Spinlock {
 public:
  Spinlock() { flag_.clear(); }

  ~Spinlock() = default;

  // -------------------------------

  inline void lock() {
    while (flag_.test_and_set(std::memory_order_acquire)) {
    }
  }

  inline void unlock() { flag_.clear(std::memory_order_release); }

  // -------------------------------

 private:
  // Atomic flag for synchronization
  std::atomic_flag flag_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_SPINLOCK_HPP_
