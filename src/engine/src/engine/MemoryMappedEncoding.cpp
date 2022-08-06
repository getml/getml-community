// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/containers/MemoryMappedEncoding.hpp"

// ----------------------------------------------------------------------------

#include "engine/utils/utils.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace containers {
// ----------------------------------------------------------------------------

void MemoryMappedEncoding::allocate() {
  btree_ = std::make_shared<BTreeType>(pool_);
  rownums_ = std::make_shared<RownumsType>(pool_);
  string_vector_ = std::make_shared<memmap::StringVector>(pool_);
}

// ----------------------------------------------------------------------------

void MemoryMappedEncoding::append(const MemoryMappedEncoding& _other,
                                  bool _include_subencoding) {
  const auto& str_vec = _other.string_vector();

  for (size_t i = 0; i < str_vec.size(); ++i) {
    (*this)[str_vec[i]];
  }

  if (_include_subencoding && _other.subencoding_) {
    append(*_other.subencoding_, true);
  }
}

// ----------------------------------------------------------------------------

void MemoryMappedEncoding::deallocate() {
  if (rownums_) {
    for (size_t i = 0; i < rownums().size(); ++i) {
      rownums()[i].second.deallocate();
    }
  }

  btree_.reset();
  rownums_.reset();
  string_vector_.reset();
}

// ----------------------------------------------------------------------------

Int MemoryMappedEncoding::insert(const strings::String& _str,
                                 const std::optional<size_t>& _opt) {
  const auto ix = static_cast<Int>(string_vector().size() + subsize_);

  const auto hash = _str.hash();

  if (!_opt) [[likely]] {
    auto p = std::make_pair(ix, memmap::VectorImpl<Int>());
    rownums().push_back(p);
    btree().insert(hash, rownums().size() - 1);
    assert_true(btree()[hash]);
    assert_true(*btree()[hash] == rownums().size() - 1);
  } else if (rownums()[*_opt].first != HASH_COLLISION) {
    // First hash collision
    assert_true(string_vector()[rownums()[*_opt].first - subsize_] != _str);
    auto p =
        std::make_pair(HASH_COLLISION, memmap::Vector<Int>(pool_).yield_impl());
    p.second.push_back(rownums()[*_opt].first);
    p.second.push_back(ix);
    rownums()[*_opt] = p;
  } else {
    // All subsequent hash collisions
    assert_true(rownums()[*_opt].second.is_allocated());
    rownums()[*_opt].second.push_back(ix);
  }

  string_vector().push_back(_str);

  return ix;
}

// ----------------------------------------------------------------------------

strings::String MemoryMappedEncoding::int_to_string(const Int _i) const {
  if (_i < 0 || static_cast<size_t>(_i) >= size()) {
    return null_value_;
  }

  assert_true(size() > 0);

  assert_true(static_cast<size_t>(_i) < size());

  if (subencoding_) {
    if (_i < subsize_) {
      return subencoding()[_i];
    } else {
      return string_vector()[_i - subsize_];
    }
  } else {
    return string_vector()[_i];
  }
}

// ----------------------------------------------------------------------------

MemoryMappedEncoding& MemoryMappedEncoding::operator=(
    const std::vector<std::string>& _vector) {
  assert_true(!subencoding_);

  clear();

  for (const auto& val : _vector) {
    (*this)[val];
  }

  return *this;
}

// ----------------------------------------------------------------------------

Int MemoryMappedEncoding::string_to_int(const strings::String& _val) {
  if (utils::NullChecker::is_null(_val)) {
    return NOT_FOUND;
  }

  // -----------------------------------
  // Note that the subencoding is const
  // - it cannot be updated.

  if (subencoding_) {
    const auto result = subencoding()[_val];

    if (result != NOT_FOUND) {
      return result;
    }
  }

  // -----------------------------------

  const auto opt = btree()[_val.hash()];

  if (!opt) {
    return insert(_val, opt);
  }

  // -----------------------------------

  const auto& p = rownums()[*opt];

  // -----------------------------------

  if (p.first != HASH_COLLISION &&
      string_vector()[p.first - subsize_] == _val) {
    return p.first;
  }

  // -----------------------------------

  if (p.first == HASH_COLLISION) {
    assert_true(p.second.is_allocated());
    for (auto i : p.second) {
      if (string_vector()[i - subsize_] == _val) {
        return i;
      }
    }
  }

  // -----------------------------------

  return insert(_val, opt);
}

// ----------------------------------------------------------------------------

Int MemoryMappedEncoding::string_to_int(const strings::String& _val) const {
  if (utils::NullChecker::is_null(_val)) {
    return NOT_FOUND;
  }

  // -----------------------------------
  // Note that the subencoding is const
  // - it cannot be updated.

  if (subencoding_) {
    const auto result = (*subencoding_)[_val];

    if (result != NOT_FOUND) {
      return result;
    }
  }

  // -----------------------------------

  const auto opt = btree()[_val.hash()];

  if (!opt) {
    return NOT_FOUND;
  }

  // -----------------------------------

  const auto& p = rownums()[*opt];

  // -----------------------------------

  if (p.first != HASH_COLLISION) {
    return string_vector()[p.first - subsize_] == _val ? p.first : NOT_FOUND;
  }

  // -----------------------------------

  assert_true(p.second.is_allocated());

  for (auto i : p.second) {
    if (string_vector()[i - subsize_] == _val) {
      return i;
    }
  }

  // -----------------------------------

  return NOT_FOUND;
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
