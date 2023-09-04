// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_VISITORWRAPPER_HPP_
#define RFL_INTERNAL_VISITORWRAPPER_HPP_

#include "rfl/Literal.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/VisitTree.hpp"
#include "rfl/internal/StringLiteral.hpp"

namespace rfl {
namespace internal {

/// Necessary for the VisitTree structure.
template <class Visitor, internal::StringLiteral... _fields>
struct VisitorWrapper {
  /// Calls the underlying visitor when required to do so.
  template <int _i, class... Args>
  inline auto visit(const Args&... _args) const {
    return (*visitor_)(name_of<Literal<_fields...>, _i>(), _args...);
  }

  /// The underlying visitor.
  const Visitor* visitor_;
};

}  // namespace internal
}  // namespace rfl

#endif  // RFL_VISIT_HPP_
