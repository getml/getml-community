// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_DEPENDENCY_TRACKER_HPP_
#define ENGINE_DEPENDENCY_TRACKER_HPP_

#include <map>
#include <memory>
#include <string>
#include <type_traits>

#include "debug/debug.hpp"
#include "fct/Ref.hpp"
#include "json/json.hpp"

namespace engine {
namespace dependency {

template <class T>
class Tracker {
 public:
  Tracker() {}

  ~Tracker() = default;

 public:
  /// Adds a new element to be tracked.
  void add(const fct::Ref<const T>& _elem);

  /// Removes all elements.
  void clear();

  /// Retrieves a deep copy of an element from the tracker, if an element
  /// containing this fingerprint exists.
  template <class FingerprintType>
  std::shared_ptr<T> retrieve(const FingerprintType& _fingerprint) const;

 private:
  /// A map keeping track of the elements.
  std::map<size_t, fct::Ref<const T>> elements_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Tracker<T>::add(const fct::Ref<const T>& _elem) {
  const auto fingerprint = _elem->fingerprint();

  const auto f_str = json::to_json(fingerprint);

  const auto f_hash = std::hash<std::string>()(f_str);

  elements_.insert_or_assign(f_hash, _elem);
}

// -------------------------------------------------------------------------

template <class T>
void Tracker<T>::clear() {
  *this = Tracker();
}

// -------------------------------------------------------------------------

template <class T>
template <class FingerprintType>
std::shared_ptr<T> Tracker<T>::retrieve(
    const FingerprintType& _fingerprint) const {
  const auto f_str = json::to_json(_fingerprint);

  const auto f_hash = std::hash<std::string>()(f_str);

  const auto it = elements_.find(f_hash);

  if (it == elements_.end()) {
    return nullptr;
  }

  const auto ptr = it->second;

  const auto fingerprint2 = ptr->fingerprint();

  const auto f2_str = json::to_json(fingerprint2);

  /// On the off-chance that there was a collision, we double-check.
  if (f_str != f2_str) {
    return nullptr;
  }

  const auto clone = ptr->clone();

  using Type = std::decay_t<decltype(clone)>;

  if constexpr (std::is_same<Type, fct::Ref<T>>()) {
    return clone.ptr();
  } else {
    return clone;
  }
}

// -------------------------------------------------------------------------
}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_TRACKER_HPP_

