// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_ENUMS_DATAUSED_HPP_
#define FASTPROP_ENUMS_DATAUSED_HPP_

#include "rfl/Literal.hpp"

namespace fastprop {
namespace enums {

/// The data to be used for aggregations of conditions. Note that "lag" is only
/// used for conditions, not aggregations.
using DataUsed =
    rfl::Literal<"categorical", "discrete", "lag", "na", "numerical",
                 "same_units_categorical", "same_units_discrete",
                 "same_units_discrete_ts", "same_units_numerical",
                 "same_units_numerical_ts", "subfeatures", "text">;

}  // namespace enums
}  // namespace fastprop

#endif  // FASTPROP_ENUMS_DATAUSED_HPP_
