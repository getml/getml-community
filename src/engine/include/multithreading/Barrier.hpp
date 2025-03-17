// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MULTITHREADING_BARRIER_HPP_
#define MULTITHREADING_BARRIER_HPP_

#include <atomic>
#include <cstddef>

namespace multithreading {
// ----------------------------------------------------------------------------

class Barrier {
 public:
  explicit Barrier(size_t _num_threads);

  ~Barrier() = default;

  // -----------------------------------------

  /// Waits until all threads have reached this point
  void wait() {
    const size_t generation = generation_;

    if (--num_threads_left_ == 0) {
      num_threads_left_ = num_threads_;
      ++generation_;
    } else {
      while (generation == generation_) {
      }
    }
  }

  // -----------------------------------------

 private:
  /// Number of times the barrier has been used
  std::atomic<size_t> generation_;

  /// Total number of threads
  size_t num_threads_;

  /// Number of threads that have not reached the barrier
  std::atomic<size_t> num_threads_left_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_BARRIER_HPP_
