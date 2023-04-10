// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_HASNAMEDTUPLEMETHODV_HPP_
#define FCT_HASNAMEDTUPLEMETHODV_HPP_

#include <type_traits>

namespace fct {

template <typename Wrapper>
using named_tuple_method_t =
    decltype(std::declval<const Wrapper>().named_tuple());

template <typename Wrapper, typename = std::void_t<>>
struct has_nt_m : std::false_type {};

template <typename Wrapper>
struct has_nt_m<Wrapper, std::void_t<named_tuple_method_t<Wrapper>>>
    : std::true_type {};

/// Utility parameter for named tuple parsing, can be used by the
/// parsers to determine whether a class or struct has a method
/// called "named_tuple".
template <typename Wrapper>
constexpr bool has_named_tuple_method_v = has_nt_m<Wrapper>::value;

template <class LIB, class = void>
struct is_available : std::false_type {};

template <class LIB>
struct is_available<LIB, std::enable_if_t<std::is_invocable_r<
                             bool, decltype(LIB::is_available)>::value>>
    : std::integral_constant<bool, LIB::is_available()> {};

template <class LIB>
constexpr bool is_available_v = is_available<LIB>::value;

}  // namespace fct

#endif  // FCT_HASNAMEDTUPLEMETHODV_HPP_
