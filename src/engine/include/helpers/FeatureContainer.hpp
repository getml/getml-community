// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_FEATURECONTAINER_HPP_
#define HELPERS_FEATURECONTAINER_HPP_

// -------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <vector>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"

// -------------------------------------------------------------------------

#include "helpers/Column.hpp"
#include "helpers/Float.hpp"
#include "helpers/Int.hpp"

// ----------------------------------------------------------------------------

namespace helpers {
// ------------------------------------------------------------------------

class FeatureContainer {
 public:
  FeatureContainer(
      const std::shared_ptr<const std::vector<Column<Float>>> _features,
      const std::shared_ptr<const std::vector<std::optional<FeatureContainer>>>
          _subcontainers)
      : features_(_features), subcontainers_(_subcontainers) {
    assert_true(features_);
    assert_true(subcontainers_);
  }

  ~FeatureContainer() = default;

  /// Trivial (const) accessor.
  const std::vector<Column<Float>>& features() const { return *features_; }

  /// Returns the number of peripheral tables
  size_t size() const { return subcontainers_->size(); }

  /// Trivial accessor
  const std::optional<FeatureContainer>& subcontainers(size_t _i) const {
    assert_true(_i < subcontainers_->size());
    return subcontainers_->at(_i);
  }

 private:
  /// The features used to add to the table holder.
  const std::shared_ptr<const std::vector<Column<Float>>> features_;

  /// The FeatureHolder used for any subtrees.
  const std::shared_ptr<const std::vector<std::optional<FeatureContainer>>>
      subcontainers_;
};

// ------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_FEATURECONTAINER_HPP_

