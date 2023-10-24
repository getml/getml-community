// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PARSING_IS_REQUIRED_HPP_
#define PARSING_IS_REQUIRED_HPP_

#include <memory>
#include <optional>
#include <type_traits>

namespace parsing {

/// Determines whether a field in a named tuple is required.
/// General case - most fields are required.
template <class T>
class is_required;

template <class T>
class is_required : public std::true_type {};

template <class T>
class is_required<std::optional<T>> : public std::false_type {};

template <class T>
class is_required<std::shared_ptr<T>> : public std::false_type {};

}  // namespace parsing

#endif  // JSON_IS_REQUIRED_HPP_
