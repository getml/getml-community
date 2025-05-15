// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "multithreading/WeakWriteLock.hpp"

namespace multithreading {

WeakWriteLock::WeakWriteLock(const rfl::Ref<ReadWriteLock>& _lock)
    : lock_(_lock), released_(true), weak_released_(false) {
  lock_->weak_write_lock();
}
/// WeakWriteLock with timeout.
WeakWriteLock::WeakWriteLock(const rfl::Ref<ReadWriteLock>& _lock,
                             const std::chrono::milliseconds _duration)
    : lock_(_lock), released_(true), weak_released_(false) {
  lock_->weak_write_lock(_duration);
}

WeakWriteLock::~WeakWriteLock() { unlock(); }

}  // namespace multithreading
