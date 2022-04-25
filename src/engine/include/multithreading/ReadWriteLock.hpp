#ifndef MULTITHREADING_READWRITELOCK_HPP_
#define MULTITHREADING_READWRITELOCK_HPP_

// ----------------------------------------------------------------------------

#include <condition_variable>
#include <mutex>
#include <thread>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "fct/Ref.hpp"

// ----------------------------------------------------------------------------

namespace multithreading {

/// We diverge from the usual design of a ReadWriteLock by introducing a
/// a weak writer: A weak writer still tolerates readers, but does
/// not tolerate other weak writer or strong writers. Also, a weak writer
/// can be upgraded to a strong writer.
class ReadWriteLock {
 public:
  ReadWriteLock()
      : active_weak_writer_exists_(false),
        active_writer_exists_(false),
        num_active_readers_(0),
        num_waiting_weak_writers_(0),
        num_waiting_writers_(0) {
    assert_true(no_active_writers());
  }

  ~ReadWriteLock() = default;

  // -------------------------------

  /// Returns true if there are currently no active readers or writers
  bool no_active_readers() const { return num_active_readers_ == 0; }

  /// Returns true if there are currently no active weak writers.
  bool no_active_weak_writers() const { return !active_weak_writer_exists_; }

  /// Returns true if there are currently no active writers
  bool no_active_writers() const { return !active_writer_exists_; }

  /// Tries to acquire a read lock.
  void read_lock() {
    std::unique_lock<std::mutex> lock(mtx_);
    reader_cond_.wait(lock, [this] { return no_active_writers(); });
    ++num_active_readers_;
    lock.unlock();
  }

  /// Tries to acquire a read lock, but with a timeout.
  void read_lock(const std::chrono::milliseconds _duration) {
    std::unique_lock<std::mutex> lock(mtx_);
    const auto acquired = reader_cond_.wait_for(
        lock, _duration, [this] { return no_active_writers(); });
    if (!acquired) {
      throw std::runtime_error("Could not acquire lock: Timeout.");
    }
    ++num_active_readers_;
    lock.unlock();
  }

  /// Releases a read lock.
  void read_unlock() {
    --num_active_readers_;

    if (num_waiting_writers_ > 0) {
      writer_cond_.notify_one();
    } else if (num_waiting_weak_writers_ > 0) {
      weak_writer_cond_.notify_one();
    }
  }

  /// Upgrades a weak write lock to a read lock.
  void upgrade_weak_write_lock() {
    assert_true(active_weak_writer_exists_);
    ++num_waiting_writers_;
    std::unique_lock<std::mutex> lock(mtx_);
    writer_cond_.wait(
        lock, [this] { return no_active_readers() && no_active_writers(); });
    --num_waiting_writers_;
    active_weak_writer_exists_ = false;
    active_writer_exists_ = true;
    lock.unlock();
  }

  /// Tries to acquire a weak write lock.
  void weak_write_lock() {
    ++num_waiting_weak_writers_;
    std::unique_lock<std::mutex> lock(mtx_);
    weak_writer_cond_.wait(lock, [this] {
      return no_active_writers() && no_active_weak_writers();
    });
    --num_waiting_weak_writers_;
    active_weak_writer_exists_ = true;
    lock.unlock();
  }

  /// Tries to acquire a weak write lock, but with a timeout.
  void weak_write_lock(const std::chrono::milliseconds _duration) {
    ++num_waiting_weak_writers_;
    std::unique_lock<std::mutex> lock(mtx_);
    const auto acquired = weak_writer_cond_.wait_for(lock, _duration, [this] {
      return no_active_writers() && no_active_weak_writers();
    });
    if (!acquired) {
      throw std::runtime_error("Could not acquire lock: Timeout.");
    }
    --num_waiting_weak_writers_;
    active_weak_writer_exists_ = true;
    lock.unlock();
  }

  /// Releases a weak write lock.
  void weak_write_unlock() {
    active_weak_writer_exists_ = false;

    if (num_waiting_writers_ > 0) {
      writer_cond_.notify_one();
    } else if (num_waiting_weak_writers_ > 0) {
      weak_writer_cond_.notify_one();
    } else {
      reader_cond_.notify_all();
    }
  }

  /// Tries to acquire a write lock.
  void write_lock() {
    ++num_waiting_writers_;
    std::unique_lock<std::mutex> lock(mtx_);
    writer_cond_.wait(lock, [this] {
      return no_active_readers() && no_active_writers() &&
             no_active_weak_writers();
    });
    --num_waiting_writers_;
    active_writer_exists_ = true;
    lock.unlock();
  }

  /// Tries to acquire a write lock, but with a timeout
  void write_lock(const std::chrono::milliseconds _duration) {
    ++num_waiting_writers_;
    std::unique_lock<std::mutex> lock(mtx_);
    const auto acquired = writer_cond_.wait_for(lock, _duration, [this] {
      return no_active_readers() && no_active_writers() &&
             no_active_weak_writers();
    });
    if (!acquired) {
      throw std::runtime_error("Could not acquire lock: Timeout.");
    }
    --num_waiting_writers_;
    active_writer_exists_ = true;
    lock.unlock();
  }

  /// Releases a write lock.
  void write_unlock() {
    active_writer_exists_ = false;

    if (num_waiting_writers_ > 0) {
      writer_cond_.notify_one();
    } else if (num_waiting_weak_writers_ > 0) {
      weak_writer_cond_.notify_one();
    } else {
      reader_cond_.notify_all();
    }
  }

  // -------------------------------

 private:
  // Whether there is a weak writer that is currently active
  std::atomic<bool> active_weak_writer_exists_;

  // Whether there is a writer that is currently active
  std::atomic<bool> active_writer_exists_;

  // Mutex variable
  std::mutex mtx_;

  // Number of readers currently active
  std::atomic<size_t> num_active_readers_;

  // Number of weak writers currently waiting
  std::atomic<size_t> num_waiting_weak_writers_;

  // Number of writers currently waiting
  std::atomic<size_t> num_waiting_writers_;

  /// Condition variable for the readers
  std::condition_variable reader_cond_;

  /// Condition variable for the weak writers
  std::condition_variable weak_writer_cond_;

  /// Condition variable for the writers
  std::condition_variable writer_cond_;
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_READWRITELOCK_HPP_
