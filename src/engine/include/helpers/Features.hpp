// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_AUTOFEATURES_HPP_
#define HELPERS_AUTOFEATURES_HPP_

#include "helpers/Column.hpp"
#include "helpers/Feature.hpp"
#include "helpers/Float.hpp"

namespace helpers {

class Features {
 public:
  typedef typename Column<Float>::Variant Variant;

 public:
  explicit Features(const std::vector<Feature<Float, false>>& _vec)
      : vec_(_vec) {}

  Features(const size_t _nrows, const size_t _ncols,
           std::optional<std::string> _temp_dir)
      : Features(make_vec(_nrows, _ncols, _temp_dir)) {}

  ~Features() = default;

  // ---------------------------------------------------------------------

 public:
  /// Access operator.
  Feature<Float, false>& at(const size_t _j) { return vec_.at(_j); }

  /// Access operator.
  Feature<Float, false> at(const size_t _j) const { return vec_.at(_j); }

  /// Access operator. Note that this note bound-checked in RELEASE mode.
  Float& at(const size_t _i, const size_t _j) {
    assert_msg(_j < vec_.size(),
               "Access out of range. _j: " + std::to_string(_j) +
                   ", number of features: " + std::to_string(vec_.size()));
    return vec_[_j][_i];
  }

  /// Access operator. Note that this note bound-checked in RELEASE mode.
  Float at(const size_t _i, const size_t _j) const {
    assert_msg(_j < vec_.size(),
               "Access out of range. _j: " + std::to_string(_j) +
                   ", number of features: " + std::to_string(vec_.size()));
    return vec_[_j][_i];
  }

  /// Returns an iterator to the beginning of vec_
  auto begin() const { return vec_.begin(); }

  /// Returns an iterator to the end of vec_
  auto end() const { return vec_.end(); }

  /// Returns the number of features
  size_t size() const { return vec_.size(); }

  /// Returns a set of safe features.
  std::vector<Feature<Float>> to_safe_features() const {
    return vec_ | std::views::transform(get_ptr<false>) |
           std::views::transform(to_feature<true>) |
           std::ranges::to<std::vector>();
  }

  /// ---------------------------------------------------------------------

 private:
  /// Transform a variant to a feature.
  template <bool _safe_mode>
  static Variant get_ptr(const Feature<Float, _safe_mode>& _feature) {
    return _feature.ptr();
  }

  /// Initializes a vector of variants. The idea is to allocate all variants
  /// first to avoid pointer misalignment in the memory-mapped version.
  static std::vector<Variant> make_variants(
      const size_t _nrows, const size_t _ncols,
      const std::shared_ptr<memmap::Pool>& _pool) {
    std::vector<typename Feature<Float>::Variant> variants;
    for (size_t col = 0; col < _ncols; ++col) {
      if (_pool) {
        variants.push_back(
            std::make_shared<memmap::Vector<Float>>(_pool, _nrows));
      } else {
        variants.push_back(std::make_shared<std::vector<Float>>(_nrows));
      }
    }
    return variants;
  }

  /// Generates vec_.
  static std::vector<Feature<Float, false>> make_vec(
      const size_t _nrows, const size_t _ncols,
      const std::optional<std::string>& _temp_dir) {
    const auto pool = _temp_dir ? std::make_shared<memmap::Pool>(*_temp_dir)
                                : std::shared_ptr<memmap::Pool>();
    const auto variants = make_variants(_nrows, _ncols, pool);
    return variants | std::views::transform(to_feature<false>) |
           std::ranges::to<std::vector>();
  }

  /// Transform a variant to a feature.
  template <bool _safe_mode>
  static Feature<Float, _safe_mode> to_feature(const Variant& _variant) {
    return Feature<Float, _safe_mode>(_variant);
  }

  /// ---------------------------------------------------------------------

 private:
  /// The vector containing the actual features.
  std::vector<Feature<Float, false>> vec_;

  // ---------------------------------------------------------------------
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_AUTOFEATURES_HPP_
