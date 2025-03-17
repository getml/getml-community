// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/FeatureContainer.hpp"

namespace helpers {

FeatureContainer::FeatureContainer(
    const std::shared_ptr<const std::vector<Column<Float>>> _features,
    const std::shared_ptr<const std::vector<std::optional<FeatureContainer>>>
        _subcontainers)
    : features_(_features), subcontainers_(_subcontainers) {
  assert_true(features_);
  assert_true(subcontainers_);
}

}  // namespace helpers
