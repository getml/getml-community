// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_GETTER_HPP_
#define FCT_GETTER_HPP_

#include <tuple>
#include <variant>

#include "fct/StringLiteral.hpp"
#include "fct/find_index.hpp"

namespace fct {

// ----------------------------------------------------------------------------

template <class NamedTupleType, int index_>
struct Getter;

// ----------------------------------------------------------------------------

/// Default case - anything that cannot be explicitly matched.
template <class NamedTupleType, int index_>
struct Getter {
  /// Retrieves the indicated value from the tuple.
  static inline auto& get(NamedTupleType& _tup) {
    return std::get<index_>(_tup.values());
  }

  /// Retrieves the indicated value from the tuple.
  static inline const auto& get_const(const NamedTupleType& _tup) {
    return std::get<index_>(_tup.values());
  }
};

// ----------------------------------------------------------------------------

/// Default case - anything that cannot be explicitly matched.
template <int index_, class... NamedTupleTypes>
struct Getter<std::variant<NamedTupleTypes...>, index_> {
 public:
  /// Retrieves the indicated value from the tuple.
  static inline auto& get(std::variant<NamedTupleTypes...>& _tup) {
    const auto apply = [](auto& _tup) {
      using NamedTupleType = decltype(_tup);
      return Getter<NamedTupleType, index_>::get(_tup);
    };
    return std::visit(_tup, apply);
  }

  /// Retrieves the indicated value from the tuple.
  static inline const auto& get_const(
      const std::variant<NamedTupleTypes...>& _tup) {
    const auto apply = [](const auto& _tup) {
      using NamedTupleType = decltype(_tup);
      return Getter<NamedTupleType, index_>::get(_tup);
    };
    return std::visit(_tup, apply);
  }
};

// ----------------------------------------------------------------------------

}  // namespace fct

#endif
