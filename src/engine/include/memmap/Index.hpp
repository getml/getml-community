#ifndef MEMMAP_INDEX_HPP_
#define MEMMAP_INDEX_HPP_

// ----------------------------------------------------------------------------

#include <cstddef>

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "memmap/BTree.hpp"
#include "memmap/VectorImpl.hpp"

// ----------------------------------------------------------------------------

namespace memmap {
// ----------------------------------------------------------------------------

template <class KeyType>
class Index {
  using ValueType = memmap::VectorImpl<size_t>;
  using BTreeType = BTree<KeyType, ValueType>;

 public:
  /// Standard constructor.
  explicit Index(const std::shared_ptr<Pool> &_pool)
      : btree_(BTreeType(_pool)), pool_(_pool) {
    assert_true(pool_);
  }

  /// Copy construtor.
  Index(const Index<KeyType> &_other) = delete;

  /// Move constructor
  Index(Index<KeyType> &&_other) noexcept
      : btree_(std::move(_other.btree_)), pool_(_other.pool_) {}

  /// Destructor
  ~Index() { deallocate(); }

 public:
  /// Deletes all data in the btree_ and declares a fresh btree_.
  void clear();

  /// Inserts a new key-rownum-pair into the index.
  void insert(const KeyType _key, const size_t _rownum);

  /// Move assignment operator.
  Index<KeyType> &operator=(Index<KeyType> &&_other) noexcept;

 public:
  /// Copy assignment operator.
  Index<KeyType> &operator=(const Index<KeyType> &_other) = delete;

  /// Access operator
  std::optional<ValueType> operator[](const KeyType _key) const {
    return btree_[_key];
  }

 private:
  /// Deallocates all VectorImpls.
  void deallocate();

 private:
  /// The underlying btree_.
  BTreeType btree_;

  /// The pool used to store the values.
  std::shared_ptr<Pool> pool_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class KeyType>
void Index<KeyType>::clear() {
  deallocate();
  btree_ = BTreeType(pool_);
}

// ----------------------------------------------------------------------------

template <class KeyType>
void Index<KeyType>::deallocate() {
  if (!btree_.is_allocated()) {
    return;
  }

  auto values = btree_.values();

  for (auto &v : values) {
    v.deallocate();
  }
}

// ----------------------------------------------------------------------------

template <class KeyType>
void Index<KeyType>::insert(const KeyType _key, const size_t _rownum) {
  auto opt = btree_[_key];

  if (opt) {
    opt->push_back(_rownum);
    btree_.insert(_key, *opt);
    return;
  }

  auto rownums = Vector<size_t>(pool_);

  rownums.push_back(_rownum);

  assert_true(btree_.is_allocated());

  btree_.insert(_key, rownums.yield_impl());
}

// ----------------------------------------------------------------------------

template <class KeyType>
Index<KeyType> &Index<KeyType>::operator=(Index<KeyType> &&_other) noexcept {
  if (&_other == this) {
    return *this;
  }

  deallocate();

  btree_ = std::move(_other.btree_);

  pool_ = _other.pool_;

  return *this;
}

// ----------------------------------------------------------------------------
}  // namespace memmap

#endif  // MEMMAP_BTREE_HPP_
