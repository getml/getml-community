// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_ENCODING_HPP_
#define CONTAINERS_ENCODING_HPP_

#include "containers/InMemoryEncoding.hpp"
#include "containers/MemoryMappedEncoding.hpp"
#include "helpers/StringIterator.hpp"

#include <memory>
#include <optional>
#include <string>
#include <variant>

namespace containers {

class Encoding {
  using InMemoryType = std::shared_ptr<InMemoryEncoding>;
  using MemoryMappedType = std::shared_ptr<MemoryMappedEncoding>;

  using ConstInMemoryType = std::shared_ptr<const InMemoryEncoding>;
  using ConstMemoryMappedType = std::shared_ptr<const MemoryMappedEncoding>;

 public:
  explicit Encoding(const std::shared_ptr<memmap::Pool>& _pool,
                    const std::shared_ptr<const Encoding> _subencoding =
                        std::shared_ptr<const Encoding>());

  ~Encoding() = default;

 public:
  /// Appends all elements of a different encoding.
  void append(const Encoding& _other, bool _include_subencoding = false) {
    bool success = append<InMemoryType>(_other, _include_subencoding);
    success = success || append<MemoryMappedType>(_other, _include_subencoding);
    assert_true(success);
  }

  /// Deletes all entries
  void clear() {
    const auto clear_pimpl = [](auto&& _pimpl) {
      assert_true(_pimpl);
      _pimpl->clear();
    };
    std::visit(clear_pimpl, pimpl_);
  }

  /// Copies a vector
  Encoding& operator=(const std::vector<std::string>& _vector) {
    const auto set_pimpl = [&_vector](auto&& _pimpl) {
      assert_true(_pimpl);
      *_pimpl = _vector;
    };
    std::visit(set_pimpl, pimpl_);
    return *this;
  }

  /// Returns the integer mapped to a string or the string mapped to an
  /// integer, updates the mapping, if necessary.
  template <class T>
  auto operator[](const T& _val) {
    const auto get = [&_val](auto&& _pimpl) {
      assert_true(_pimpl);
      return (*_pimpl)[_val];
    };
    return std::visit(get, pimpl_);
  }

  /// Returns the integer mapped to a string or the string mapped to an
  /// integer (const version).
  template <class T>
  auto operator[](const T& _val) const {
    const auto handle = [&_val](auto&& _ptr) {
      using PimplType = std::decay_t<decltype(_ptr)>;

      if constexpr (std::is_same_v<PimplType, InMemoryType>)
        return (*ConstInMemoryType(_ptr))[_val];

      if constexpr (std::is_same_v<PimplType, MemoryMappedType>)
        return (*ConstMemoryMappedType(_ptr))[_val];
    };
    return std::visit(handle, pimpl_);
  }

  /// Number of encoded elements
  size_t size() const {
    const auto get_size = [](auto&& _pimpl) -> size_t {
      assert_true(_pimpl);
      return _pimpl->size();
    };
    return std::visit(get_size, pimpl_);
  }

  /// The temporary directory (only relevant for the MemoryMappedEncoding)
  std::optional<std::string> temp_dir() const {
    if (std::holds_alternative<InMemoryType>(pimpl_)) {
      return std::nullopt;
    }

    assert_true(std::holds_alternative<MemoryMappedType>(pimpl_));
    const ConstMemoryMappedType pimpl = std::get<MemoryMappedType>(pimpl_);
    assert_true(pimpl);
    return pimpl->temp_dir();
  }

  /// Get the vector containing the strings.
  inline helpers::StringIterator strings() const {
    const auto make_string_iterator =
        [](auto&& _pimpl) -> helpers::StringIterator {
      assert_true(_pimpl);
      const auto func = [_pimpl](const size_t _i) -> strings::String {
        return (*_pimpl)[_i];
      };
      return helpers::StringIterator(func, _pimpl->size());
    };
    return std::visit(make_string_iterator, pimpl_);
  }

 private:
  /// Appends to the encoding.
  template <class PtrType>
  bool append(const Encoding& _other, bool _include_subencoding = false) {
    if (std::holds_alternative<PtrType>(pimpl_)) {
      assert_true(std::holds_alternative<PtrType>(_other.pimpl_));
      const auto pimpl = std::get<PtrType>(pimpl_);
      const auto other = std::get<PtrType>(_other.pimpl_);
      assert_true(other);
      pimpl->append(*other, _include_subencoding);
      return true;
    }
    return false;
  }

  /// Initializes the encoding.
  template <class EncodingType>
  void init(const std::shared_ptr<memmap::Pool>& _pool,
            const std::shared_ptr<const Encoding> _subencoding) {
    using PtrType = std::shared_ptr<EncodingType>;

    assert_true(!_subencoding ||
                std::holds_alternative<PtrType>(_subencoding->pimpl_));

    const auto subencoding =
        _subencoding ? std::get<PtrType>(_subencoding->pimpl_) : PtrType();

    if constexpr (std::is_same<PtrType, InMemoryType>()) {
      pimpl_ = std::make_shared<EncodingType>(subencoding);
    }

    if constexpr (std::is_same<PtrType, MemoryMappedType>()) {
      pimpl_ = std::make_shared<EncodingType>(_pool, subencoding);
    }
  }

 private:
  /// Abstracts over an InMemoryEncoding and MemoryMappedEncoding.
  std::variant<InMemoryType, MemoryMappedType> pimpl_;
};

}  // namespace containers

#endif  // CONTAINERS_ENCODING_HPP_
