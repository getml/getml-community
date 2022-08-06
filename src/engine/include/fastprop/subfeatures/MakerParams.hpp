// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FASTPROP_SUBFEATURES_MAKERPARAMS_HPP_
#define FASTPROP_SUBFEATURES_MAKERPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "fastprop/Hyperparameters.hpp"

// ----------------------------------------------------------------------------

#include "fastprop/subfeatures/FastPropContainer.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace subfeatures {
// ------------------------------------------------------------------------

struct MakerParams {
  /// The container for the fast prop algorithm - needed for transformation
  /// only.
  const std::shared_ptr<const FastPropContainer> fast_prop_container_ = nullptr;

  /// The hyperparameters used to construct the FastProp algorithm.
  const std::shared_ptr<const Hyperparameters> hyperparameters_;

  /// The logger used to log the progress.
  const std::shared_ptr<const logging::AbstractLogger> logger_;

  /// The peripheral tables used for training.
  const std::vector<containers::DataFrame> peripheral_;

  /// Names of the peripheral tables, as referenced by the placeholder.
  const std::shared_ptr<const std::vector<std::string>> peripheral_names_;

  /// The placeholder used.
  const containers::Placeholder placeholder_;

  /// The population table used for training.
  const containers::DataFrame population_;

  /// The prefix used for the generated features.
  const std::string prefix_;

  /// The row index container used to fit the text columns.
  const std::optional<helpers::RowIndexContainer> row_index_container_ =
      std::nullopt;

  /// The temporary directory to be used to place the memory mapping (when set
  /// to nullopt, everything will be done in memory)
  const std::optional<std::string> temp_dir_;

  /// The word index container used to transform the text columns.
  const helpers::WordIndexContainer word_index_container_;
};

// ------------------------------------------------------------------------
}  // namespace subfeatures
}  // namespace fastprop

#endif  // FASTPROP_SUBFEATURES_MAKERPARAMS_HPP_
