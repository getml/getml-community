// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_DEPENDENCY_TRACKER_HPP_
#define ENGINE_DEPENDENCY_TRACKER_HPP_

#include <Poco/JSON/Object.h>

#include <map>
#include <memory>
#include <string>

#include "debug/debug.hpp"
#include "engine/JSON.hpp"
#include "fct/Ref.hpp"

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
  std::shared_ptr<T> retrieve(const Poco::JSON::Object::Ptr _fingerprint) const;

 private:
  /// A map keeping track of the elements.
  std::map<size_t, fct::Ref<const T>> elements_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Tracker<T>::add(const fct::Ref<const T>& _elem) {
  const auto fingerprint = _elem->fingerprint();

  assert_true(fingerprint);

  const auto f_str = JSON::stringify(*fingerprint);

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
std::shared_ptr<T> Tracker<T>::retrieve(
    const Poco::JSON::Object::Ptr _fingerprint) const {
  assert_true(_fingerprint);

  const auto f_str = JSON::stringify(*_fingerprint);

  const auto f_hash = std::hash<std::string>()(f_str);

  const auto it = elements_.find(f_hash);

  if (it == elements_.end()) {
    return nullptr;
  }

  const auto ptr = it->second;

  const auto fingerprint2 = ptr->fingerprint();

  assert_true(fingerprint2);

  const auto f2_str = JSON::stringify(*fingerprint2);

  /// On the off-chance that there was a collision, we double-check.
  if (f_str != f2_str) {
    return nullptr;
  }

  return ptr->clone();
}

// -------------------------------------------------------------------------
}  // namespace dependency
}  // namespace engine

#endif  // ENGINE_DEPENDENCY_TRACKER_HPP_

