// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_HASNAMEDTUPLETYPEV_HPP_
#define FCT_HASNAMEDTUPLETYPEV_HPP_

#include <utility>

namespace fct {
template <class Wrapper>
class HasNamedTupleType {
 private:
  struct SizeOne {
    char a[1];
  };
  struct SizeTwo {
    char a[2];
  };

  template <class U>
  static SizeOne foo(typename U::NamedTupleType*);

  template <class U>
  static SizeTwo foo(...);

 public:
  static constexpr bool value =
      sizeof(foo<Wrapper>(nullptr)) == sizeof(SizeOne);
};

/// Utility parameter for named tuple parsing, can be used by the
/// parsers to determine whether a class or struct defines a type
/// called "NamedTupleType".
template <typename Wrapper>
constexpr bool has_named_tuple_type_v = HasNamedTupleType<Wrapper>::value;

}  // namespace fct

#endif  // FCT_HASNAMEDTUPLETYPEV_HPP_
