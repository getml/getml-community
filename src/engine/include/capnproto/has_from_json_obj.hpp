// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPNPROTO_HASFROMCAPNPROTOOBJ_HPP_
#define CAPNPROTO_HASFROMCAPNPROTOOBJ_HPP_

#include <Poco/Dynamic/Var.h>

#include <type_traits>

namespace capnproto {

template <class T, class = void>
struct has_from_capnproto_obj : std::false_type {};

template <class T>
struct has_from_capnproto_obj<
    T, std::enable_if_t<std::is_invocable_r<T, decltype(T::from_capnproto_obj),
                                            Poco::Dynamic::Var>::value>>
    : std::true_type {};

/// Utility parameter for named tuple parsing, can be used by the
/// parsers to determine whether a class or struct defines a static method
/// called "has_capnproto_obj".
template <class T>
constexpr bool has_from_capnproto_obj_v = has_from_capnproto_obj<T>::value;

}  // namespace capnproto

#endif  // FCT_HASFROMCAPNPROTOOBJ_HPP_
