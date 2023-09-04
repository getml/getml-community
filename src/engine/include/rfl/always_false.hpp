// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_ALWAYSFALSE_HPP_
#define RFL_ALWAYSFALSE_HPP_

namespace rfl {

/// To be used inside visitor patterns
template <class>
inline constexpr bool always_false_v = false;

}  // namespace rfl

#endif  // RFL_ALWAYSFALSE_HPP_
