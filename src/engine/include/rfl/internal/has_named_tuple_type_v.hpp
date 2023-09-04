// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_HASNAMEDTUPLETYPEV_HPP_
#define RFL_HASNAMEDTUPLETYPEV_HPP_

#include <cstdint>
#include <utility>

namespace rfl {
namespace internal {

template <class Wrapper>
class HasNamedTupleType {
 private:
  template <class U>
  static std::int64_t foo(...);

  template <class U>
  static std::int32_t foo(typename U::NamedTupleType*);

 public:
  static constexpr bool value =
      sizeof(foo<Wrapper>(nullptr)) == sizeof(std::int32_t);
};

/// Utility parameter for named tuple parsing, can be used by the
/// parsers to determine whether a class or struct defines a type
/// called "NamedTupleType".
template <typename Wrapper>
constexpr bool has_named_tuple_type_v = HasNamedTupleType<Wrapper>::value;

}  // namespace internal
}  // namespace rfl

#endif  // RFL_HASNAMEDTUPLETYPEV_HPP_
