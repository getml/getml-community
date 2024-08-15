// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PREPROCESSED_HPP_
#define ENGINE_PIPELINES_PREPROCESSED_HPP_

#include <rfl/Ref.hpp>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "containers/DataFrame.hpp"

namespace engine {
namespace pipelines {

struct Preprocessed {
  /// The fingerprints of the data frames used for fitting.
  const rfl::Ref<const std::vector<commands::Fingerprint>> df_fingerprints_;

  /// The modified peripheral data frames (after applying the preprocessors)
  const std::vector<containers::DataFrame> peripheral_dfs_;

  /// The modified population data frame (after applying the preprocessors)
  const containers::DataFrame population_df_;

  /// The preprocessors used in this pipeline.
  const std::vector<rfl::Ref<const preprocessors::Preprocessor>> preprocessors_;

  /// The fingerprints of the preprocessor used for fitting.
  const rfl::Ref<const std::vector<commands::Fingerprint>>
      preprocessor_fingerprints_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PREPROCESSED_HPP_
