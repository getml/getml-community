// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FINGERPRINTS_HPP_
#define ENGINE_PIPELINES_FINGERPRINTS_HPP_

#include <rfl/Field.hpp>
#include <rfl/Ref.hpp>
#include <vector>

#include "commands/Fingerprint.hpp"

namespace engine {
namespace pipelines {

struct Fingerprints {
  /// The fingerprints of the data frames used for fitting.
  rfl::Field<"df_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      df_fingerprints;

  /// The fingerprints of the preprocessors used for fitting.
  rfl::Field<"preprocessor_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      preprocessor_fingerprints;

  /// The fingerprints of the feature learners used for fitting.
  rfl::Field<"fl_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      fl_fingerprints;

  /// The fingerprints of the feature selectors used for fitting.
  rfl::Field<"fs_fingerprints_",
             rfl::Ref<const std::vector<commands::Fingerprint>>>
      fs_fingerprints;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FINGERPRINTS_HPP_
