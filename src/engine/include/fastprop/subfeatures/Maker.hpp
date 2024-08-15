// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_SUBFEATURES_MAKER_HPP_
#define FASTPROP_SUBFEATURES_MAKER_HPP_

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "fastprop/subfeatures/FastPropContainer.hpp"
#include "fastprop/subfeatures/MakerParams.hpp"

namespace fastprop {
namespace subfeatures {

class Maker {
 public:
  /// Fits any and all FastProp algorithms whereever
  /// propositionalization flag is activated.
  static std::pair<std::shared_ptr<const FastPropContainer>,
                   helpers::FeatureContainer>
  fit(const MakerParams& _params);

  /// Generates a new table holder by transforming the input data.
  static helpers::FeatureContainer transform(const MakerParams& _params);

 private:
  /// Finds the correct index matching the joined table to peripheral_names.
  static size_t find_peripheral_ix(const MakerParams& _params, const size_t _i);

  /// First any and all required FastProp algorithms.
  static std::shared_ptr<const FastPropContainer> fit_fast_prop_container(
      const helpers::TableHolder& _table_holder, const MakerParams& _params);

  /// Generates the subcontainers.
  static std::shared_ptr<typename FastPropContainer::Subcontainers>
  make_subcontainers(const helpers::TableHolder& _table_holder,
                     const MakerParams& _params);

  /// Generates the params for fitting a subcontainer.
  static MakerParams make_params(const MakerParams& _params, const size_t _i);

  /// Generates placeholder used to generate the propositionalization.
  static std::shared_ptr<const helpers::Placeholder> make_placeholder(
      const MakerParams& _params);

  /// Generates the features in the transform step.
  static std::shared_ptr<std::vector<helpers::Column<Float>>>
  transform_make_features(const MakerParams& _params);

  /// Generates the subcontainers in the transform step.
  static std::shared_ptr<std::vector<std::optional<helpers::FeatureContainer>>>
  transform_make_subcontainers(const MakerParams& _params);
};

}  // namespace subfeatures
}  // namespace fastprop

#endif  // FASTPROP_SUBFEATURES_MAKER_HPP_
