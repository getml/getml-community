// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_HASFROMJSONOBJ_HPP_
#define JSON_HASFROMJSONOBJ_HPP_

#include <Poco/Dynamic/Var.h>

#include <type_traits>

namespace json {

template <class T, class = void>
struct has_from_json_obj : std::false_type {};

template <class T>
struct has_from_json_obj<
    T, std::enable_if_t<std::is_invocable_r<T, decltype(T::from_json_obj),
                                            Poco::Dynamic::Var>::value>>
    : std::true_type {};

/// Utility parameter for named tuple parsing, can be used by the
/// parsers to determine whether a class or struct defines a static method
/// called "has_json_obj".
template <class T>
constexpr bool has_from_json_obj_v = has_from_json_obj<T>::value;

}  // namespace json

#endif  // FCT_HASFROMJSONOBJ_HPP_
