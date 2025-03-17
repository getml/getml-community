// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "memmap/FreeBlocksTracker.hpp"

#include "debug/assert_msg.hpp"
#include "debug/assert_true.hpp"

namespace memmap {

// ----------------------------------------------------------------------------

std::pair<size_t, bool> FreeBlocksTracker::allocate_block(
    const size_t _block_size) {
#ifndef NDEBUG
  for (size_t i = 1; i < free_blocks_.size(); ++i) {
    assert_true(free_blocks_[i - 1].begin_ < free_blocks_[i - 1].end_);
    assert_true(free_blocks_[i].begin_ < free_blocks_[i].end_);
    assert_true(free_blocks_[i].begin_ > free_blocks_[i - 1].end_);
  }
#endif

  const auto [block_num, ok] = find_free_block(_block_size);

  if (!ok) {
    return std::make_pair(0, false);
  }

  assert_true(block_num < free_blocks_.size());

  auto& block = free_blocks_.at(block_num);

  assert_true(block.end_ > block.begin_);
  assert_true(block.end_ - block.begin_ >= _block_size);

  if (block.end_ - block.begin_ > _block_size) [[likely]] {
    const auto begin = block.begin_;
    block.begin_ += _block_size;
    return std::make_pair(begin, true);
  }

  const auto begin = block.begin_;
  free_blocks_.erase(free_blocks_.begin() + block_num);
  return std::make_pair(begin, true);
}

// ----------------------------------------------------------------------------

void FreeBlocksTracker::extend_block(const size_t _page_num, const size_t _by) {
#ifndef NDEBUG
  for (size_t i = 1; i < free_blocks_.size(); ++i) {
    assert_true(free_blocks_[i - 1].begin_ < free_blocks_[i - 1].end_);
    assert_true(free_blocks_[i].begin_ < free_blocks_[i].end_);
    assert_true(free_blocks_[i].begin_ > free_blocks_[i - 1].end_);
  }
#endif

  for (auto it = free_blocks_.begin(); it != free_blocks_.end(); ++it) {
    auto& block = *it;
    assert_true(block.begin_ <= _page_num);
    if (block.begin_ == _page_num) {
      assert_true(block.end_ > block.begin_);

      assert_msg(block.end_ - block.begin_ >= _by,
                 "block.begin_: " + std::to_string(block.begin_) +
                     ", block.end_: " + std::to_string(block.end_) +
                     ", _by: " + std::to_string(_by));
      if (block.end_ - block.begin_ == _by) {
        free_blocks_.erase(it);
        return;
      }
      block.begin_ += _by;
      return;
    }
  }
  assert_true(false);
}

// ----------------------------------------------------------------------------

std::pair<size_t, bool> FreeBlocksTracker::find_free_block(
    const size_t _block_size) const {
  for (size_t i = 0; i < free_blocks_.size(); ++i) {
    const auto& block = free_blocks_[i];
    assert_true(block.end_ > block.begin_);
    if (block.end_ - block.begin_ >= _block_size) {
      return std::make_pair(i, true);
    }
  }
  return std::make_pair(0, false);
}

// ----------------------------------------------------------------------------

void FreeBlocksTracker::free_block(const size_t _begin, const size_t _end) {
  assert_true(_end > _begin);
#ifndef NDEBUG
  for (size_t i = 1; i < free_blocks_.size(); ++i) {
    assert_true(free_blocks_[i - 1].begin_ < free_blocks_[i - 1].end_);
    assert_true(free_blocks_[i].begin_ < free_blocks_[i].end_);
    assert_true(free_blocks_[i].begin_ > free_blocks_[i - 1].end_);
  }
#endif

  for (auto it = free_blocks_.begin(); it != free_blocks_.end(); ++it) {
    auto& block = *it;

    if (block.end_ == _begin && it != free_blocks_.end() &&
        (it + 1)->begin_ == _end) {
      (it + 1)->begin_ = block.begin_;
      free_blocks_.erase(it);
      return;
    }

    if (block.end_ == _begin) {
      block.end_ = _end;
      return;
    }

    if (block.begin_ == _end) {
      block.begin_ = _begin;
      return;
    }

    if (block.begin_ > _end) {
      const auto new_block = FreeBlock{.begin_ = _begin, .end_ = _end};
      free_blocks_.insert(it, new_block);
      return;
    }
  }

  free_blocks_.push_back(FreeBlock{.begin_ = _begin, .end_ = _end});
}

// ----------------------------------------------------------------------------

void FreeBlocksTracker::increase_pool_size(const size_t _new_pool_size) {
  if (free_blocks_.size() != 0 && free_blocks_.back().end_ == pool_size_) {
    free_blocks_.back().end_ = _new_pool_size;
  } else {
    free_blocks_.push_back(
        FreeBlock{.begin_ = pool_size_, .end_ = _new_pool_size});
  }
  pool_size_ = _new_pool_size;
}

// ----------------------------------------------------------------------------

}  // namespace memmap
