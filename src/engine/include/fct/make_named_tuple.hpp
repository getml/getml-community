// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_MAKENAMEDTUPLE_HPP_
#define FCT_MAKENAMEDTUPLE_HPP_

#include "fct/NamedTuple.hpp"

namespace fct {

/// Convenience constructor that doesn't require you
/// to explitly defined the field types.
template <class... FieldTypes>
auto make_named_tuple(FieldTypes... _args) {
  return NamedTuple<FieldTypes...>(std::forward<FieldTypes>(_args)...);
}

}  // namespace fct

#endif  // FCT_MAKENAMEDTUPLE_HPP_
