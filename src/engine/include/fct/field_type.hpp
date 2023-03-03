// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_FIELD_TYPE_HPP_
#define FCT_FIELD_TYPE_HPP_

#include <tuple>

#include "fct/NamedTuple.hpp"
#include "fct/StringLiteral.hpp"
#include "fct/find_index.hpp"

namespace fct {

/// Finds the type of the field signified by _field_name
template <StringLiteral _field_name, class NamedTupleType>
struct FieldType {
  static constexpr int field_ix_ =
      fct::find_index<_field_name, typename NamedTupleType::Fields>();

  using Type =
      typename std::tuple_element<field_ix_,
                                  typename NamedTupleType::Fields>::type::Type;
};

template <StringLiteral _field_name, class NamedTupleType>
using field_type_t = typename FieldType<_field_name, NamedTupleType>::Type;

}  // namespace fct

#endif
