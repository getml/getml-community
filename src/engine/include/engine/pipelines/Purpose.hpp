// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PURPOSE_HPP_
#define ENGINE_PIPELINES_PURPOSE_HPP_

#include "fct/Literal.hpp"

namespace engine {
namespace pipelines {

using Purpose = fct::Literal<"feature_selectors_", "predictors_">;

}
}  // namespace engine

#endif

