// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MULTITHREADING_WRITELOCK_HPP_
#define MULTITHREADING_WRITELOCK_HPP_

// ----------------------------------------------------------------------------

#include <memory>

// ----------------------------------------------------------------------------

#include <rfl/Ref.hpp>

// ----------------------------------------------------------------------------

#include "multithreading/ReadWriteLock.hpp"

// ----------------------------------------------------------------------------

namespace multithreading {

class WriteLock {
 public:
  /// WriteLock without a timeout.
  explicit WriteLock(const rfl::Ref<ReadWriteLock>& _lock)
      : lock_(_lock), released_(false) {
    lock_->write_lock();
  }

  /// WriteLock with timeout.
  WriteLock(const rfl::Ref<ReadWriteLock>& _lock,
            const std::chrono::milliseconds _duration)
      : lock_(_lock), released_(false) {
    lock_->write_lock(_duration);
  }

  ~WriteLock() { unlock(); }

  // -------------------------------

  /// Because of the boolean variable, this operator is forbidden.
  WriteLock& operator=(const WriteLock& _other) = delete;

  /// Lock the ReadWriteLock.
  void lock() {
    assert_true(released_);
    released_ = false;
    lock_->write_lock();
  }

  /// Unlock the ReadWriteLock.
  void unlock() {
    if (!released_) {
      lock_->write_unlock();
      released_ = true;
    }
  }

 private:
  /// Lock to the WriteLock.
  const rfl::Ref<ReadWriteLock> lock_;

  /// Whether the Acquirer has been released.
  bool released_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_WRITELOCK_HPP_
