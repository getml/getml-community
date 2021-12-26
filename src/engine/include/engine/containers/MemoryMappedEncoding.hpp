#ifndef ENGINE_CONTAINERS_MEMORYMAPPEDENCODING_HPP_
#define ENGINE_CONTAINERS_MEMORYMAPPEDENCODING_HPP_

// -------------------------------------------------------------------------

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "memmap/memmap.hpp"
#include "strings/strings.hpp"

// -------------------------------------------------------------------------

#include "engine/Int.hpp"

// -------------------------------------------------------------------------

namespace engine {
namespace containers {
// -------------------------------------------------------------------------

class MemoryMappedEncoding {
  using BTreeType = memmap::BTree<size_t, size_t>;
  using RownumsType = memmap::Vector<std::pair<Int, memmap::VectorImpl<Int>>>;

  constexpr static Int NOT_FOUND = -1;
  constexpr static Int HASH_COLLISION = -2;

 public:
  MemoryMappedEncoding(
      const std::shared_ptr<memmap::Pool>& _pool,
      const std::shared_ptr<const MemoryMappedEncoding> _subencoding =
          std::shared_ptr<const MemoryMappedEncoding>())
      : null_value_("NULL"),
        pool_(_pool),
        subencoding_(_subencoding),
        subsize_(_subencoding ? _subencoding->size() : 0) {
    assert_true(pool_);
    allocate();
  }

  /// Move constructor
  MemoryMappedEncoding(MemoryMappedEncoding&& _other) noexcept = delete;

  /// Copy constructor.
  MemoryMappedEncoding(const MemoryMappedEncoding& _other) = delete;

  ~MemoryMappedEncoding() { deallocate(); };

  // -------------------------------

  /// Appends all elements of a different encoding.
  void append(const MemoryMappedEncoding& _other,
              bool _include_subencoding = false);

  /// Copies a vector
  MemoryMappedEncoding& operator=(const std::vector<std::string>& _vector);

  /// Move assignment operator.
  MemoryMappedEncoding& operator=(MemoryMappedEncoding&& _other) = delete;

  /// Copy assignment operator.
  MemoryMappedEncoding& operator=(const MemoryMappedEncoding& _other) = delete;

  // -------------------------------

  /// Deletes all entries
  void clear() {
    deallocate();
    allocate();
  }

  /// Returns the integer mapped to a string.
  template <class T>
  typename std::conditional<std::is_same<T, std::string>::value ||
                                std::is_same<T, strings::String>::value,
                            Int, strings::String>::type
  operator[](const T& _val) {
    if constexpr (std::is_same<T, std::string>()) {
      return string_to_int(strings::String(_val));
    }

    if constexpr (std::is_same<T, strings::String>()) {
      return string_to_int(_val);
    }

    if constexpr (!std::is_same<T, std::string>() &&
                  !std::is_same<T, strings::String>()) {
      return int_to_string(_val);
    }
  }

  /// Returns the integer mapped to a string (const version).
  template <class T>
  typename std::conditional<std::is_same<T, std::string>::value ||
                                std::is_same<T, strings::String>::value,
                            Int, strings::String>::type
  operator[](const T& _val) const {
    if constexpr (std::is_same<T, std::string>()) {
      return string_to_int(strings::String(_val));
    }

    if constexpr (std::is_same<T, strings::String>()) {
      return string_to_int(_val);
    }

    if constexpr (!std::is_same<T, std::string>() &&
                  !std::is_same<T, strings::String>()) {
      return int_to_string(_val);
    }
  }

  /// Trivial (const) accessor
  std::shared_ptr<memmap::Pool> pool() const { return pool_; }

  /// Number of encoded elements
  size_t size() const { return subsize_ + string_vector().size(); }

  /// Trivial (const) accessor
  const memmap::StringVector& string_vector() const {
    assert_true(string_vector_);
    return *string_vector_;
  }

  /// Trivial (const) accessor
  const std::string& temp_dir() const {
    assert_true(pool_);
    return pool_->temp_dir();
  }

  // -------------------------------

 private:
  /// Trivial (private) accessor
  BTreeType& btree() {
    assert_true(btree_);
    return *btree_;
  }

  /// Trivial (private) accessor
  const BTreeType& btree() const {
    assert_true(btree_);
    return *btree_;
  }

  /// Trivial (private) accessor
  RownumsType& rownums() {
    assert_true(rownums_);
    return *rownums_;
  }

  /// Trivial (private) accessor
  const RownumsType& rownums() const {
    assert_true(rownums_);
    return *rownums_;
  }

  /// Trivial (private) accessor
  const MemoryMappedEncoding& subencoding() const {
    assert_true(subencoding_);
    return *subencoding_;
  }

  /// Trivial (private) accessor
  memmap::StringVector& string_vector() {
    assert_true(string_vector_);
    return *string_vector_;
  }

  // -------------------------------

 private:
  /// Allocates the ressources.
  void allocate();

  /// Deallocates all ressources and deletes the folders containing the mapped
  /// files.
  void deallocate();

  /// Adds an integer to map_ and vector_, assuming it is not already included
  Int insert(const strings::String& _val, const std::optional<size_t>& _opt);

  /// Returns the string mapped to an integer.
  strings::String int_to_string(const Int _i) const;

  /// Returns the integer mapped to a string.
  Int string_to_int(const strings::String& _val);

  /// Returns the integer mapped to a string (const version).
  Int string_to_int(const strings::String& _val) const;

  // -------------------------------

 private:
  /// For fast lookup
  std::shared_ptr<BTreeType> btree_;

  /// The null value (needed because strings are returned by reference).
  const strings::String null_value_;

  /// The pool containing the data.
  std::shared_ptr<memmap::Pool> pool_;

  /// The rownums
  std::shared_ptr<RownumsType> rownums_;

  /// A subencoding can be used to separate the existing encoding from new
  /// data. Under some circumstance, we want to avoid the global encoding
  /// being edited, such as when we process requests in parallel.
  const std::shared_ptr<const MemoryMappedEncoding> subencoding_;

  /// Maps integers to strings
  std::shared_ptr<memmap::StringVector> string_vector_;

  /// The size of the subencoding at the time this encoding was created.
  const size_t subsize_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_MEMORYMAPPEDENCODING_HPP_
