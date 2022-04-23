#ifndef MEMMAP_FREEBLOCKSTRACKER_HPP_
#define MEMMAP_FREEBLOCKSTRACKER_HPP_

// ----------------------------------------------------------------------------

#include <cstddef>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "memmap/FreeBlock.hpp"

// ----------------------------------------------------------------------------

namespace memmap {

/// Keeps track of free blocks, so we can find them more quickly.
class FreeBlocksTracker {
 public:
  FreeBlocksTracker() : pool_size_(0) {}

  ~FreeBlocksTracker() = default;

 public:
  /// Finds a block of the required block
  /// size, in number of pages, marks it as allocated.
  /// Returns 0, false, if no block could be found.
  std::pair<size_t, bool> allocate_block(const size_t _block_size);

  /// Extends the size of an already existing allocated block.
  void extend_block(const size_t _page_num, const size_t _by);

  /// Marks a block of memory as free.
  void free_block(const size_t _begin, const size_t _end);

  /// Increases the pool size and
  /// adapts the free blocks accordingly.
  void increase_pool_size(const size_t _new_pool_size);

 private:
  /// Finds a free block of the appropriate size.
  std::pair<size_t, bool> find_free_block(const size_t _block_size) const;

 private:
  /// Keeps track of the blocks that are not currently
  /// occupied.
  std::vector<FreeBlock> free_blocks_;

  /// The size of the underlying memory pool,
  /// in number of pages
  size_t pool_size_;
};

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_FREEBLOCKSTRACKER_HPP_
