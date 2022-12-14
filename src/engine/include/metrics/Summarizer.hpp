// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef METRICS_SUMMARIZER_HPP_
#define METRICS_SUMMARIZER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <cstdint>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/Int.hpp"

// ----------------------------------------------------------------------------

namespace metrics {
// -------------------------------------------------------------------------

class Summarizer {
  // ---------------------------------------------------------------------

 public:
  /// Calculates the plots needed to analyze a categorical column.
  static Poco::JSON::Object calc_categorical_column_plot(
      const size_t _num_bins, const std::vector<strings::String>& _vec);

  /// Calculates the plots needed to analyze a categorical column.
  static Poco::JSON::Object calc_categorical_column_plot(
      const size_t _num_bins, const std::vector<strings::String>& _vec,
      const std::vector<Float>& _target);

  /// Calculates the pearson r between features and
  /// a set of targets.
  static Poco::JSON::Object calculate_feature_correlations(
      const Features& _features, const size_t _nrows, const size_t _ncols,
      const std::vector<const Float*>& _targets);

  /// Calculates the plots needed to analyze the feature.
  static Poco::JSON::Object calculate_feature_plots(
      const Features& _features, const size_t _nrows, const size_t _ncols,
      const size_t _num_bins, const std::vector<const Float*>& _targets);

  // ---------------------------------------------------------------------

 private:
  /// Calculates the average targets, which are displayed in the feature view.
  static Poco::JSON::Array::Ptr calculate_average_targets(
      const std::vector<Float>& _minima, const std::vector<Float>& _step_sizes,
      const std::vector<size_t>& _actual_num_bins,
      const std::vector<std::vector<Int>>& _feature_densities,
      const Features& _features, const size_t _nrows, const size_t _ncols,
      const std::vector<const Float*>& _targets);

  /// Helper function for calculating labels, which
  /// are needed for column densities and average targets.
  static Poco::JSON::Array::Ptr calculate_labels(
      const std::vector<Float>& _minima, const std::vector<Float>& _step_sizes,
      const std::vector<size_t>& _actual_num_bins,
      const std::vector<std::vector<Int>>& _feature_densities,
      const Features& _features, const size_t _nrows, const size_t _ncols);

  /// Helper function for calculating the step size and number of bins, which
  /// is needed for column densities and average targets.
  static void calculate_step_sizes_and_num_bins(
      const std::vector<Float>& _minima, const std::vector<Float>& _maxima,
      const Float _num_bins, std::vector<Float>* _step_sizes,
      std::vector<size_t>* _actual_num_bins);

  /// Helper function for identifying the correct bin, which
  /// finds the minimum and maximum.
  static void find_min_and_max(const Features& _features, const size_t _nrows,
                               const size_t _ncols, std::vector<Float>* _minima,
                               std::vector<Float>* _maxima);

  /// Helper function for identifying the correct bin, which
  /// is needed for column densities and average targets.
  static size_t identify_bin(const size_t _num_bins, const Float _step_size,
                             const Float _val, const Float _min);

  /// Applies the binning strategy for the categorical frequency counts.
  static Poco::JSON::Object make_object(
      const size_t _num_bins,
      const std::vector<std::pair<strings::String, Int>>& _counts);

  /// Applies the binning strategy for the categorical correlation plots.
  static Poco::JSON::Object make_object(
      const size_t _num_bins,
      const std::vector<std::pair<Float, Float>>& _pairs);

  // ---------------------------------------------------------------------

 private:
  /// Helper function
  template <typename T>
  static const T& get(const size_t _i, const size_t _j, const size_t _ncols,
                      const std::vector<T>& _vec) {
    assert_true(_j < _ncols);
    assert_true(_i * _ncols + _j < _vec.size());
    return _vec[_i * _ncols + _j];
  }

  /// Helper function
  template <typename T>
  static T& get(const size_t _i, const size_t _j, const size_t _ncols,
                std::vector<T>* _vec) {
    assert_true(_j < _ncols);
    assert_true(_i * _ncols + _j < _vec->size());
    return (*_vec)[_i * _ncols + _j];
  }

  /// Helper function
  static const Float get(const size_t _i, const size_t _j,
                         const Features& _features) {
    assert_true(_j < _features.size());
    assert_true(_i < _features[_j].size());
    return _features[_j][_i];
  }

  // ---------------------------------------------------------------------
};

// -------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_SUMMARIZER_HPP_
