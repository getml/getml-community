// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_ALGORITHM_MEMOIZATION_HPP_
#define FASTPROP_ALGORITHM_MEMOIZATION_HPP_

// ----------------------------------------------------------------------------

#include <algorithm>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "fastprop/Float.hpp"
#include "fastprop/containers/AbstractFeature.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace algorithm {

class Memoization {
 public:
  Memoization() {}

  ~Memoization() = default;

 public:
  /// Memorizes the range, if necessary.
  template <class RangeType>
  void memorize_numerical(const containers::AbstractFeature& _abstract_feature,
                          RangeType _range) {
    if (is_same(abstract_feature_numerical_, _abstract_feature)) {
      return;
    }
    if constexpr (std::ranges::sized_range<RangeType>) {
      numerical_.resize(std::ranges::size(_range));
      std::copy(_range.begin(), _range.end(), numerical_.begin());
    } else {
      numerical_.clear();
      std::copy(_range.begin(), _range.end(), std::back_inserter(numerical_));
    }
    abstract_feature_numerical_.emplace(_abstract_feature);
  }

  /// Memorizes the range, if necessary.
  template <class RangeType>
  void memorize_pairs(const containers::AbstractFeature& _abstract_feature,
                      RangeType _range) {
    if (is_same(abstract_feature_pairs_, _abstract_feature)) {
      return;
    }
    if constexpr (std::ranges::sized_range<RangeType>) {
      pairs_.resize(std::ranges::size(_range));
      std::copy(_range.begin(), _range.end(), pairs_.begin());
    } else {
      pairs_.clear();
      std::copy(_range.begin(), _range.end(), std::back_inserter(pairs_));
    }
    abstract_feature_pairs_.emplace(_abstract_feature);
  }

  /// Pointer to the beginning of the cached data
  const Float* numerical_begin() const { return numerical_.data(); }

  /// Pointer to the end of the cached data
  const Float* numerical_end() const {
    return numerical_.data() + numerical_.size();
  }

  /// Pointer to the beginning of the cached data
  const std::pair<Float, Float>* pairs_begin() const { return pairs_.data(); }

  /// Pointer to the end of the cached data
  const std::pair<Float, Float>* pairs_end() const {
    return pairs_.data() + pairs_.size();
  }

  /// Resets the abstract features
  void reset() {
    abstract_feature_numerical_.reset();
    abstract_feature_pairs_.reset();
  }

 private:
  /// TODO: Better logic for the conditions
  /// Whether we have already cached a similar abstract feature.
  bool is_same(const std::optional<containers::AbstractFeature>& _af1,
               const containers::AbstractFeature& _af2) const {
    if (!_af1) {
      return false;
    }
    return (_af1->categorical_value_ == _af2.categorical_value_ &&
            _af1->data_used_ == _af2.data_used_ &&
            _af1->input_col_ == _af2.input_col_ &&
            _af1->output_col_ == _af2.output_col_ &&
            _af1->peripheral_ == _af2.peripheral_ &&
            conditions_match(*_af1, _af2));
  }

  /// Determines whether the conditions are identical.
  bool conditions_match(const containers::AbstractFeature& _af1,
                        const containers::AbstractFeature& _af2) const {
    if (_af1.conditions_.size() != _af2.conditions_.size()) {
      return false;
    }
    for (size_t i = 0; i < _af1.conditions_.size(); ++i) {
      if (_af1.conditions_[i] != _af2.conditions_[i]) {
        return false;
      }
    }
    return true;
  }

 private:
  /// Abstract descriptions of the feature which has been cached.
  std::optional<containers::AbstractFeature> abstract_feature_numerical_;

  /// Abstract descriptions of the feature which has been cached.
  std::optional<containers::AbstractFeature> abstract_feature_pairs_;

  /// The cached numerical.
  std::vector<Float> numerical_;

  /// The cached data for aggregations that rely on
  /// time stamps as well
  std::vector<std::pair<Float, Float>> pairs_;
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_ALGORITHM_MEMOIZATION_HPP_
