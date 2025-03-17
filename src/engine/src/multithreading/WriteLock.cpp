// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "multithreading/WriteLock.hpp"

namespace multithreading {

WriteLock::WriteLock(const rfl::Ref<ReadWriteLock>& _lock)
    : lock_(_lock), released_(false) {
  lock_->write_lock();
}

/// WriteLock with timeout.
WriteLock::WriteLock(const rfl::Ref<ReadWriteLock>& _lock,
                     const std::chrono::milliseconds _duration)
    : lock_(_lock), released_(false) {
  lock_->write_lock(_duration);
}

WriteLock::~WriteLock() { unlock(); }

}  // namespace multithreading
