// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_VISIT_HPP_
#define FCT_VISIT_HPP_

#include "fct/Literal.hpp"
#include "fct/StringLiteral.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/VisitTree.hpp"

namespace fct {

/// Necessary for the VisitTree structure.
template <class Visitor, StringLiteral... _fields>
struct VisitorWrapper {
  /// Calls the underlying visitor when required to do so.
  template <int _i, class... Args>
  inline auto visit(const Args&... _args) const {
    return (*visitor_)(name_of<Literal<_fields...>, _i>(), _args...);
  }

  /// The underlying visitor.
  const Visitor* visitor_;
};

/// Implements the visitor pattern for Literals.
template <class Visitor, StringLiteral... _fields, class... Args>
inline auto visit(const Visitor& _visitor, const Literal<_fields...> _literal,
                  const Args&... _args) {
  constexpr int size = sizeof...(_fields);
  using WrapperType = VisitorWrapper<Visitor, _fields...>;
  const auto wrapper = WrapperType(&_visitor);
  return VisitTree::visit<0, size, WrapperType>(wrapper, _literal.value(),
                                                _args...);
}

/// Implements the visitor pattern for TaggedUnions.
template <class Visitor, StringLiteral _discriminator, class... Args>
inline auto visit(const Visitor& _visitor,
                  const TaggedUnion<_discriminator, Args...> _tagged_union) {
  return std::visit(_visitor, _tagged_union.variant_);
}

}  // namespace fct

#endif  // FCT_VISIT_HPP_
