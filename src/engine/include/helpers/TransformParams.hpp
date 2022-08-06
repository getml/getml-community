// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_TRANSFORMPARAMS_HPP_
#define HELPERS_TRANSFORMPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "logging/logging.hpp"

// ----------------------------------------------------------------------------

#include "helpers/DataFrame.hpp"
#include "helpers/FeatureContainer.hpp"
#include "helpers/WordIndexContainer.hpp"

// ----------------------------------------------------------------------------

namespace helpers {
// ----------------------------------------------------------------------------

struct TransformParams {
  /// Contains the features trained by the propositionalization.
  const std::optional<const FeatureContainer> feature_container_;

  /// Indicates the features we want to extract.
  const std::vector<size_t> index_;

  /// Uses to log progress.
  const std::shared_ptr<const logging::AbstractLogger> logger_;

  /// The peripheral tables used.
  const std::vector<DataFrame> peripheral_;

  /// The population table used.
  const DataFrame population_;

  /// The temporary directory to be used to place the memory mapping (when set
  /// to nullopt, everything will be done in memory)
  const std::optional<std::string> temp_dir_;

  /// Contains the word indices of the text fields.
  const WordIndexContainer word_indices_;
};

// ----------------------------------------------------------------------------
}  // namespace helpers

// ----------------------------------------------------------------------------
#endif  // HELPERS_TRANSFORMPARAMS_HPP_

