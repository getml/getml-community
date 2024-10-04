// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MULTIREL_MULTITHREADING_READLOCK_HPP_
#define MULTIREL_MULTITHREADING_READLOCK_HPP_

// ----------------------------------------------------------------------------

#include <chrono>

// ----------------------------------------------------------------------------

#include <rfl/Ref.hpp>

// ----------------------------------------------------------------------------

#include "multithreading/ReadWriteLock.hpp"

// ----------------------------------------------------------------------------

namespace multithreading {

class ReadLock {
 public:
  /// ReadLock without a timeout.
  explicit ReadLock(const rfl::Ref<ReadWriteLock>& _lock)
      : lock_(_lock), released_(false) {
    lock_->read_lock();
  }

  /// Read lock with timeout.
  ReadLock(const rfl::Ref<ReadWriteLock>& _lock,
           const std::chrono::milliseconds _duration)
      : lock_(_lock), released_(false) {
    lock_->read_lock(_duration);
  }

  ~ReadLock() { unlock(); }

  // -------------------------------

  /// Because of the boolean variable, this operator is forbidden.
  ReadLock& operator=(const ReadLock& _other) = delete;

  /// Lock the ReadWriteLock.
  void lock() {
    assert_true(released_);
    released_ = false;
    lock_->read_lock();
  }

  /// Unlock the ReadWriteLock.
  void unlock() {
    if (!released_) {
      lock_->read_unlock();
      released_ = true;
    }
  }

 private:
  /// Lock to the ReadLock.
  const rfl::Ref<ReadWriteLock> lock_;

  /// Whether the Acquirer has been released.
  bool released_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTIREL_MULTITHREADING_READLOCK_HPP_
