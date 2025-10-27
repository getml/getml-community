// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MULTITHREADING_WEAKWRITELOCK_HPP_
#define MULTITHREADING_WEAKWRITELOCK_HPP_

#include "multithreading/ReadWriteLock.hpp"

#include <rfl/Ref.hpp>

namespace multithreading {

class WeakWriteLock {
 public:
  /// WeakWriteLock without a timeout.
  explicit WeakWriteLock(const rfl::Ref<ReadWriteLock>& _lock);
  /// WeakWriteLock with timeout.
  WeakWriteLock(const rfl::Ref<ReadWriteLock>& _lock,
                const std::chrono::milliseconds _duration);

  ~WeakWriteLock();

  /// Because of the boolean variable, this operator is forbidden.
  WeakWriteLock& operator=(const WeakWriteLock& _other) = delete;

  /// Lock the ReadWriteLock.
  void lock() {
    assert_true(released_);
    assert_true(weak_released_);
    weak_released_ = false;
    lock_->weak_write_lock();
  }

  /// Unlock the ReadWriteLock.
  void unlock() {
    if (!released_) {
      lock_->write_unlock();
      released_ = true;
    }

    if (!weak_released_) {
      lock_->weak_write_unlock();
      weak_released_ = true;
    }
  }

  /// Upgrade from a weak write lock to a strong write lock.
  void upgrade() {
    assert_true(!weak_released_);
    assert_true(released_);
    lock_->upgrade_weak_write_lock();
    weak_released_ = true;
    released_ = false;
  }

 private:
  /// Lock to the WeakWriteLock.
  const rfl::Ref<ReadWriteLock> lock_;

  /// Whether the acquirer has been released.
  bool released_;

  /// Whether the weak acquirer has been released.
  bool weak_released_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_WEAKWRITELOCK_HPP_
