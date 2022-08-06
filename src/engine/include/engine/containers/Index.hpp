// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_CONTAINERS_INDEX_HPP_
#define ENGINE_CONTAINERS_INDEX_HPP_

namespace engine {
namespace containers {
// -------------------------------------------------------------------------

template <class T, class Hash = std::hash<T>>
class Index {
  using InMemoryType = std::unordered_map<T, std::vector<size_t>, Hash>;
  using MemoryMappedType = memmap::Index<T>;

 public:
  using MapType = std::variant<InMemoryType, MemoryMappedType>;

 public:
  Index(const std::shared_ptr<memmap::Pool>& _pool)
      : begin_(0),
        map_(_pool ? std::make_shared<MapType>(MemoryMappedType(_pool))
                   : std::make_shared<MapType>(InMemoryType())) {}

  ~Index() = default;

  // -------------------------------

  /// Recalculates the index.
  void calculate(const Column<T>& _key);

  /// Returns a pointer to the beginning and end of the rownums, or two
  /// nullptrs, if the key is not found.
  std::pair<const size_t*, const size_t*> find(const T _key) const;

  // -------------------------------

  /// Returns a const copy to the underlying map.
  std::shared_ptr<MapType> map() const { return map_; }

  // -------------------------------

 private:
  /// Calculates the index, once the map type has been evaluated.
  template <class KnownMapType>
  void calculate_known_type(const Column<T>& _key, KnownMapType* _map);

  // Determines whether this is a NULL value
  bool is_null(const T& _val) const;

  /// Insert a key-rownum-pair into _map;
  void insert(const T _key, const size_t _rownum, InMemoryType* _map) const;

  // -------------------------------

 private:
  /// Insert a key-rownum-pair into _map;
  void insert(const T _key, const size_t _rownum,
              MemoryMappedType* _map) const {
    _map->insert(_key, _rownum);
  }

  // -------------------------------

 private:
  /// Stores the first row number for which we do not have an index.
  size_t begin_;

  /// Performs the role of an "index" over the keys
  std::shared_ptr<MapType> map_;

  // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T, class Hash>
void Index<T, Hash>::calculate(const Column<T>& _key) {
  assert_true(map_);

  if (std::holds_alternative<InMemoryType>(*map_)) {
    calculate_known_type(_key, &std::get<InMemoryType>(*map_));
    return;
  }

  if (std::holds_alternative<MemoryMappedType>(*map_)) {
    calculate_known_type(_key, &std::get<MemoryMappedType>(*map_));
    return;
  }

  assert_true(false);
}

// -------------------------------------------------------------------------

template <class T, class Hash>
template <class KnownMapType>
void Index<T, Hash>::calculate_known_type(const Column<T>& _key,
                                          KnownMapType* _map) {
  if (_key.size() < begin_) {
    _map->clear();
    begin_ = 0;
  }

  for (size_t i = begin_; i < _key.nrows(); ++i) {
    if (!is_null(_key[i])) {
      insert(_key[i], i, _map);
    }
  }

  begin_ = _key.nrows();
}

// -------------------------------------------------------------------------

template <class T, class Hash>
std::pair<const size_t*, const size_t*> Index<T, Hash>::find(
    const T _key) const {
  assert_true(map_);

  if (std::holds_alternative<InMemoryType>(*map_)) {
    const auto& idx = std::get<InMemoryType>(*map_);

    const auto it = idx.find(_key);

    if (it == idx.end()) {
      return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
    }

    return std::make_pair(it->second.data(),
                          it->second.data() + it->second.size());
  }

  if (std::holds_alternative<MemoryMappedType>(*map_)) {
    const auto& idx = std::get<MemoryMappedType>(*map_);

    const auto opt = idx[_key];

    if (!opt) {
      return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
    }

    const auto begin = opt->data();

    return std::make_pair(begin, begin + opt->size());
  }

  assert_true(false);

  return std::make_pair<const size_t*, const size_t*>(nullptr, nullptr);
}

// -------------------------------------------------------------------------

template <class T, class Hash>
void Index<T, Hash>::insert(const T _key, const size_t _rownum,
                            InMemoryType* _map) const {
  auto it = _map->find(_key);

  if (it == _map->end()) {
    (*_map)[_key] = {_rownum};
  } else {
    it->second.push_back(_rownum);
  }
}

// -------------------------------------------------------------------------

template <class T, class Hash>
bool Index<T, Hash>::is_null(const T& _val) const {
  if constexpr (std::is_same<T, Int>()) {
    return _val < 0;
  }

  if constexpr (std::is_same<T, Float>()) {
    return (std::isnan(_val) || std::isinf(_val));
  }

  if constexpr (std::is_same<T, strings::String>()) {
    return utils::NullChecker::is_null(_val);
  }

  return false;
}

// -------------------------------------------------------------------------

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_INDEX_HPP_
