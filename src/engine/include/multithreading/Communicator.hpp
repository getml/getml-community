// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MULTITHREADING_COMMUNICATOR_HPP_
#define MULTITHREADING_COMMUNICATOR_HPP_

#include "multithreading/Barrier.hpp"
#include "multithreading/Spinlock.hpp"

#include <cstddef>
#include <thread>
#include <vector>

namespace multithreading {
// ----------------------------------------------------------------------------

class Communicator {
 public:
  explicit Communicator(size_t _num_threads)
      : barrier_(_num_threads),
        checkpoint_(true),
        main_thread_id_(std::this_thread::get_id()),
        num_threads_(_num_threads),
        num_threads_left_(_num_threads) {}

  ~Communicator() = default;

  // -----------------------------------------

  /// Waits until all threads have reached this point
  inline void barrier() { barrier_.wait(); }

  /// Forces all threads to throw an exception, if any thread does not pass
  /// true.
  inline void checkpoint(const bool _ok) {
    if (!_ok) {
      checkpoint_ = false;
    }

    barrier();

    if (!checkpoint_) {
      throw std::runtime_error("Interrupted.");
    }
  }

  /// Accessor to the shared data
  template <class T>
  inline T* global_data() {
    return reinterpret_cast<T*>(global_data_.data());
  }

  /// Const accessor to the shared data
  template <class T>
  inline const T* global_data_const() {
    return reinterpret_cast<const T*>(global_data_.data());
  }

  /// Locks the spinlock
  inline void lock() { spinlock_.lock(); }

  /// Returns the id of the main thread
  inline const std::thread::id& main_thread_id() { return main_thread_id_; }

  /// Returns the number of threads
  inline const size_t& num_threads() { return num_threads_; }

  /// Returns the number of threads that haven't reached this point
  inline std::atomic<size_t>& num_threads_left() { return num_threads_left_; }

  /// To ensure compatability with MPI
  inline size_t rank() {
    return ((std::this_thread::get_id() == main_thread_id_) ? (0) : (1));
  }

  /// Resizes the global (shared) data
  template <class T>
  inline void resize(const size_t _size) {
    if (global_data_.size() < _size * sizeof(T)) {
      global_data_.resize(_size * sizeof(T));
    }
  }

  /// Unlocks the spinlock
  inline void unlock() { spinlock_.unlock(); }

  // -----------------------------------------

 private:
  /// Barrier used for the communicator
  Barrier barrier_;

  /// Whether we are allowed to pass the checkpoint.
  std::atomic<bool> checkpoint_;

  /// Storage for the global data. Note that
  /// sizeof(char) is 1 by definition.
  std::vector<char> global_data_;

  /// Id of the main thread
  std::thread::id main_thread_id_;

  /// Total number of threads
  size_t num_threads_;

  /// Number of threads that have not updated the global data
  /// in the current generation.
  std::atomic<size_t> num_threads_left_;

  /// Spinlock protecting the global data
  Spinlock spinlock_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_COMMUNICATOR_HPP_
