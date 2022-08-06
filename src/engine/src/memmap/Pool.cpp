// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "memmap/Pool.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <tuple>

// ----------------------------------------------------------------------------

#include <Poco/File.h>
#include <Poco/TemporaryFile.h>

// ----------------------------------------------------------------------------

namespace memmap {

Pool::Pool(const std::string& _temp_dir)
    : fd_data_(-1),
      fd_pages_(-1),
      num_pages_(0),
      page_size_(static_cast<size_t>(getpagesize())),
      pages_(nullptr),
      temp_dir_(_temp_dir) {
  std::tie(path_pages_, fd_pages_) = create_file(_temp_dir);
  std::tie(path_data_, fd_data_) = create_file(_temp_dir);

  resize_pool(1000);
}

// ----------------------------------------------------------------------------

Pool::~Pool() {
  unmap();
  remove_file(fd_data_, path_data_);
  remove_file(fd_pages_, path_pages_);
}

// ----------------------------------------------------------------------------

void Pool::allocate_page(const size_t _block_size, const size_t _page_num) {
  auto& new_page = pages_[_page_num];
  new_page.block_size_ = _block_size;
  new_page.is_allocated_ = true;
}

// ----------------------------------------------------------------------------

size_t Pool::allocate_block(const size_t _block_size,
                            const size_t _current_page) {
  assert_true(_block_size > 0);

  const bool extend_current_block =
      current_block_can_be_extended(_block_size, _current_page);

  if (extend_current_block) {
    assert_true(_current_page < num_pages_);
    const auto page_num = _current_page + pages_[_current_page].block_size_;
    const auto by = _block_size - pages_[_current_page].block_size_;
    free_blocks_tracker_.extend_block(page_num, by);
    allocate_page(_block_size, _current_page);
    return _current_page;
  }

  while (true) {
    const auto [page_num, found] =
        free_blocks_tracker_.allocate_block(_block_size);

    if (!found) {
      resize_pool(num_pages_ * 2);
      continue;
    }

    allocate_page(_block_size, page_num);

    if (_current_page == NOT_ALLOCATED) {
      return page_num;
    }

    move_data_to_new_block(_current_page, page_num);

    return page_num;
  }

  assert_true(false);

  return 0;
}

// ----------------------------------------------------------------------------

void Pool::check_space_left(const size_t _additional_bytes) const {
  std::error_code err_code;
  const auto info = std::filesystem::space(temp_dir_, err_code);
  const auto available_bytes = static_cast<size_t>(info.available);
  if (available_bytes < _additional_bytes) {
    throw std::runtime_error(
        "Could not allocate memory-mapped ressources: Not enough disk space "
        "available.");
  }
}

// ----------------------------------------------------------------------------

std::pair<std::string, int> Pool::create_file(
    const std::string& _temp_dir) const {
  Poco::File(_temp_dir).createDirectories();

  auto file = Poco::TemporaryFile(_temp_dir);

  file.keep();

  const auto path = file.path();

  const auto fd = open(path.c_str(), O_RDWR | O_CREAT);

  if (fd == -1) {
    throw std::runtime_error("Could not create '" + path +
                             "', errno: " + std::to_string(errno));
  }

  return std::make_pair(path, fd);
}

// ----------------------------------------------------------------------------

bool Pool::current_block_can_be_extended(const size_t _block_size,
                                         const size_t _current_page) const {
  if (_current_page == NOT_ALLOCATED) {
    return false;
  }

  if (_current_page + _block_size >= num_pages_) {
    return false;
  }

  if (_block_size <= pages_[_current_page].block_size_) [[unlikely]] {
    return true;
  }

  const auto is_free = [](const Page& _page) -> bool {
    return !_page.is_allocated_;
  };

  return std::all_of(pages_ + _current_page + pages_[_current_page].block_size_,
                     pages_ + _current_page + _block_size, is_free);
}

// ----------------------------------------------------------------------------

void Pool::deallocate(const size_t _page_num) {
  auto& old_page = pages_[_page_num];

  assert_true(old_page.block_size_ != 0);
  assert_true(old_page.is_allocated_);

  free_blocks_tracker_.free_block(_page_num, _page_num + old_page.block_size_);

  old_page = Page();
}

// ----------------------------------------------------------------------------

void Pool::init_pages(const size_t _first_new_page,
                      const size_t _last_new_page) {
  for (size_t i = _first_new_page; i < _last_new_page; ++i) {
    pages_[i] = Page();
  }
}

// ----------------------------------------------------------------------------

char* Pool::memmap(const int _fd, const size_t _length) const {
  assert_true(_fd >= 0);

  auto addr = mmap(NULL, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);

  assert_msg(addr != MAP_FAILED, "_fd: " + std::to_string(_fd) +
                                     ", _length: " + std::to_string(_length) +
                                     " " + strerror(errno));

  if (addr == MAP_FAILED) {
    throw std::runtime_error("Could not map onto file: " +
                             std::string(strerror(errno)));
  }

  return reinterpret_cast<char*>(addr);
}

// ----------------------------------------------------------------------------

void Pool::move_data_to_new_block(const size_t _old_page_num,
                                  const size_t _new_page_num) {
  assert_true(_old_page_num < num_pages_);
  assert_true(_new_page_num < num_pages_);
  assert_true(pages_[_old_page_num].block_size_ <
              pages_[_new_page_num].block_size_);
  assert_true(pages_[_old_page_num].is_allocated_);
  assert_true(pages_[_new_page_num].is_allocated_);

  const auto& old_page = pages_[_old_page_num];

  const auto input_begin = data_ + _old_page_num * page_size_;
  const auto input_end = input_begin + old_page.block_size_ * page_size_;
  const auto output_begin = data_ + _new_page_num * page_size_;

  std::copy(input_begin, input_end, output_begin);

  deallocate(_old_page_num);
}

// ----------------------------------------------------------------------------

void Pool::remap(const size_t _num_pages) {
  data_ = memmap(fd_data_, _num_pages * page_size_);
  pages_ =
      reinterpret_cast<Page*>(memmap(fd_pages_, _num_pages * sizeof(Page)));
}

// ----------------------------------------------------------------------------

void Pool::remove_file(const int _fd, const std::string& _path) const {
  close(_fd);

  auto file = Poco::File(_path);

  file.remove();
}

// ----------------------------------------------------------------------------

void Pool::resize_file(const int _fd, const size_t _num_bytes) const {
  assert_true(_fd >= 0);

  const auto err = ftruncate(_fd, _num_bytes);

  if (err == -1) {
    throw std::runtime_error("Could not allocate disk space: " +
                             std::string(strerror(errno)));
  }
}

// ----------------------------------------------------------------------------

void Pool::resize_pool(const size_t _num_pages) {
  assert_true(_num_pages > num_pages_);
  const auto additional_bytes =
      (_num_pages - num_pages_) * (page_size_ + sizeof(Page));
  check_space_left(additional_bytes);
  if (num_pages_ != 0) {
    unmap();
  }
  resize_file(fd_pages_, _num_pages * sizeof(Page));
  resize_file(fd_data_, _num_pages * page_size_);
  remap(_num_pages);
  init_pages(num_pages_, _num_pages);
  num_pages_ = _num_pages;
  free_blocks_tracker_.increase_pool_size(num_pages_);
}

// ----------------------------------------------------------------------------

void Pool::unmap() {
  munmap(data_, num_pages_ * page_size_);
  munmap(pages_, num_pages_ * sizeof(Page));
}

// ----------------------------------------------------------------------------
}  // namespace memmap
