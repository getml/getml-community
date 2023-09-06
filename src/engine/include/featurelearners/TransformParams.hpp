// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_TRANSFORMPARAMS_HPP_
#define FEATURELEARNERS_TRANSFORMPARAMS_HPP_

#include <cstddef>
#include <vector>

#include "commands/DataFramesOrViews.hpp"
#include "communication/communication.hpp"
#include "featurelearners/FitParams.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/define_named_tuple.hpp"

namespace featurelearners {

using TransformParams =
    rfl::define_named_tuple_t<FitParams,

                              /// Indicates which features we want to generate.
                              rfl::Field<"index_", std::vector<size_t>>>;

}  // namespace featurelearners

#endif  // FEATURELEARNERS_TRANSFORMPARAMS_HPP_

