// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_NAME_T_HPP_
#define RFL_NAME_T_HPP_

#include <type_traits>

namespace rfl {

/// Convenience class to retrieve the name of a field.
template <class FieldType>
using name_t = typename std::decay_t<FieldType>::Name;

}  // namespace rfl

#endif
