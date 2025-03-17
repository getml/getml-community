// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef MEMMAP_POOL_HPP_
#define MEMMAP_POOL_HPP_

#include "debug/assert_true.hpp"
#include "memmap/FreeBlocksTracker.hpp"
#include "memmap/Page.hpp"

#include <cstddef>
#include <limits>
#include <string>

namespace memmap {

class Pool {
 public:
  constexpr static size_t NOT_ALLOCATED = std::numeric_limits<size_t>::max();

 public:
  explicit Pool(const std::string &_temp_dir);

  /// Move constructor
  Pool(Pool &&_other) noexcept = delete;

  /// Copy constructor.
  Pool(const Pool &_other) = delete;

  ~Pool();

 public:
  /// Returns the addr of the page indicated by pagenum.
  template <class T>
  T *addr(const size_t _page_num) const {
    assert_true(_page_num < num_pages_);
    assert_true(pages_[_page_num].is_allocated_);
    return reinterpret_cast<T *>(data_ + _page_num * page_size_);
  }

  /// Allocates enough space to contain at lease _num_elements elements.
  /// Returns the page_num of the first page in the new block.
  template <class T>
  size_t allocate(const size_t _num_elements,
                  const size_t _current_page = NOT_ALLOCATED) {
    const auto num_bytes = _num_elements * sizeof(T);
    const auto block_size = num_bytes % page_size_ == 0
                                ? num_bytes / page_size_
                                : num_bytes / page_size_ + 1;
    return allocate_block(block_size, _current_page);
  }

  /// Returns the capacity of the memory block indicated by _page_num.
  template <class T>
  size_t capacity(const size_t _page_num) const {
    assert_true(_page_num < num_pages_);
    assert_true(pages_[_page_num].is_allocated_);
    assert_true(pages_[_page_num].block_size_ > 0);
    const auto num_bytes = pages_[_page_num].block_size_ * page_size_;
    return num_bytes / sizeof(T);
  }

  /// Deallocates the block beginning with the page signified by _page_num.
  void deallocate(const size_t _page_num);

  /// Move assignment operator.
  Pool &operator=(Pool &&_other) = delete;

  /// Copy assignment operator.
  Pool &operator=(const Pool &_other) = delete;

  /// The number of bytes currently in the pool.
  size_t num_bytes() const { return page_size_ * num_pages_; }

  /// The number of pages currently in the pool.
  size_t num_pages() const { return num_pages_; }

  /// The size of a single page.
  size_t page_size() const { return page_size_; }

  /// Trivial (const) accessor
  const std::string &temp_dir() const { return temp_dir_; }

 private:
  /// Allocates a block of pages.
  size_t allocate_block(const size_t _block_size, const size_t _current_page);

  /// Allocates the first page in the block.
  void allocate_page(const size_t _block_size, const size_t _page_num);

  /// Make sure that enough space is left on the machine.
  void check_space_left(const size_t _num_bytes) const;

  /// Creates a new file (either for the raw data or for the pages).
  std::pair<std::string, int> create_file(const std::string &_temp_dir) const;

  /// Whether the current block can just be extended
  bool current_block_can_be_extended(const size_t _num_pages,
                                     const size_t _current_page) const;

  /// Initializes the pages (RAII does not work for memory mapping,
  /// so we need to do this manually).
  void init_pages(const size_t _first_new_page, const size_t _last_new_page);

  /// Convenience wrapper around memmap.
  char *memmap(const int _fd, const size_t _num_elements) const;

  /// Copies the data from the block beginning with _old_page_num to
  /// _new_page_num, then deallocated _old_page_num.
  void move_data_to_new_block(const size_t _old_page_num,
                              const size_t _new_page_num);

  /// Remaps the memory pages after a file has been resized.
  void remap(const size_t _num_pages);

  /// Removes a file (either for the raw data or the pages).
  void remove_file(const int _fd, const std::string &_path) const;

  /// Resizes a file.
  void resize_file(const int _fd, const size_t _num_bytes) const;

  /// Resizes the entire pool.
  void resize_pool(const size_t _num_pages);

  /// Unmaps the memory pages, so the pool can be resized.
  void unmap();

 private:
  /// Memory-mapped pointer to the actual data.
  char *data_;

  /// The file descriptor of the file containing the actual data.
  int fd_data_;

  /// The file descriptor of the file containing the pages.
  int fd_pages_;

  /// Helps us find free blocks more quickly
  FreeBlocksTracker free_blocks_tracker_;

  /// The number of pages currently allocated.
  size_t num_pages_;

  /// The size of a single page, in bytes.
  const size_t page_size_;

  /// Memory-mapped pointer to the pages.
  Page *pages_;

  /// The path of the file containing the actual data.
  std::string path_data_;

  /// The path of the file containing the pages.
  std::string path_pages_;

  /// The directory where the data is stored.
  const std::string temp_dir_;
};

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_POOL_HPP_
