// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FINGERPRINTS_HPP_
#define ENGINE_PIPELINES_FINGERPRINTS_HPP_

#include <vector>

#include "commands/Fingerprint.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace pipelines {

using Fingerprints = fct::NamedTuple<
    /// The fingerprints of the data frames used for fitting.
    fct::Field<"df_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The fingerprints of the preprocessors used for fitting.
    fct::Field<"preprocessor_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The fingerprints of the feature learners used for fitting.
    fct::Field<"fl_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>,

    /// The fingerprints of the feature selectors used for fitting.
    fct::Field<"fs_fingerprints_",
               fct::Ref<const std::vector<commands::Fingerprint>>>>;

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FINGERPRINTS_HPP_

