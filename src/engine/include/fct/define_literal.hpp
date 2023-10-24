// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_DEFINELITERAL_HPP_
#define FCT_DEFINELITERAL_HPP_

#include "fct/Literal.hpp"

namespace fct {

/// Allows you to combine several literals.
template <class... LiteralTypes>
struct define_literal;

/// General case
template <StringLiteral... _content1, StringLiteral... _content2, class... Tail>
struct define_literal<Literal<_content1...>, Literal<_content2...>, Tail...> {
  using type = typename define_literal<Literal<_content1..., _content2...>,
                                       Tail...>::type;
};

/// Special case - only a single literal is left
template <StringLiteral... _content>
struct define_literal<Literal<_content...>> {
  using type = Literal<_content...>;
};

template <class... LiteralTypes>
using define_literal_t = typename define_literal<LiteralTypes...>::type;

}  // namespace fct

#endif  // FCT_DEFINELITERAL_HPP_
