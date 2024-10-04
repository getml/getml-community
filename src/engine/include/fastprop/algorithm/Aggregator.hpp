// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_ALGORITHM_AGGREGATOR_HPP_
#define FASTPROP_ALGORITHM_AGGREGATOR_HPP_

#include <memory>
#include <ranges>
#include <rfl/Ref.hpp>
#include <utility>

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/algorithm/Memoization.hpp"
#include "fastprop/containers/DataFrame.hpp"
#include "fastprop/containers/Features.hpp"
#include "fastprop/containers/Match.hpp"
#include "fastprop/containers/SQLMaker.hpp"
#include "helpers/Aggregations.hpp"
#include "textmining/WordIndex.hpp"

namespace fastprop {
namespace algorithm {

class Aggregator {
 public:
  typedef std::vector<std::shared_ptr<const textmining::WordIndex>> WordIndices;

 public:
  /// Applies the aggregation defined in _abstract feature to each of the
  /// matches.
  static Float apply_aggregation(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::optional<containers::Features> &_subfeatures,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

 public:
  /// Whether the aggregation is an aggregation that relies on the
  /// first-last-logic.
  static bool is_first_last(const enums::Aggregation _agg) {
    return containers::SQLMaker::is_first_last(_agg);
  }

 private:
  /// Applies an aggregation to a categorical column.
  static Float apply_categorical(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Determines whether a condition is true w.r.t. a match.
  static bool apply_condition(const containers::Condition &_condition,
                              const containers::Match &_match);

  /// Applies the aggregation to a discrete column.
  static Float apply_discrete(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies a COUNT aggregation
  static Float apply_not_applicable(
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies the aggregation to a numerical column.
  static Float apply_numerical(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies the aggregation to categorical columns with the same unit.
  static Float apply_same_units_categorical(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies the aggregation to discrete columns with the same unit.
  static Float apply_same_units_discrete(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies the aggregation to numerical columns with the same unit.
  static Float apply_same_units_numerical(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies the aggregation to a subfeature.
  static Float apply_subfeatures(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const containers::Features &_subfeatures,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

  /// Applies the aggregation to text fields.
  static Float apply_text(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization);

 private:
  /// Aggregates the range from _begin to _end, applying the _aggregation.
  template <class IteratorType>
  static Float aggregate_categorical_range(
      const IteratorType _begin, const IteratorType _end,
      const enums::Aggregation _aggregation) {
    switch (_aggregation.value()) {
      case enums::Aggregation::value_of<"COUNT DISTINCT">():
        return helpers::Aggregations::count_distinct(_begin, _end);

      case enums::Aggregation::value_of<"COUNT MINUS COUNT DISTINCT">():
        return helpers::Aggregations::count(_begin, _end) -
               helpers::Aggregations::count_distinct(_begin, _end);

      default:
        assert_msg(false, "Unknown aggregation for categorical column: '" +
                              _aggregation.name() + "'");
        return 0.0;
    }
  }

  /// Aggregates the range from _begin to _end, applying the _aggregation.
  template <class IteratorType>
  static Float aggregate_first_last(const IteratorType _begin,
                                    const IteratorType _end,
                                    const enums::Aggregation _aggregation) {
    if (std::distance(_begin, _end) <= 0) {
      return 0.0;
    }

    constexpr Float t1s = 1.0;
    constexpr Float t1m = 60.0 * t1s;
    constexpr Float t1h = 60.0 * t1m;
    constexpr Float t1d = 24.0 * t1h;
    constexpr Float t7d = 7.0 * t1d;
    constexpr Float t30d = 30.0 * t1d;
    constexpr Float t90d = 90.0 * t1d;
    constexpr Float t365d = 365.0 * t1d;

    switch (_aggregation.value()) {
      case enums::Aggregation::value_of<"FIRST">():
        return helpers::Aggregations::first(_begin, _end);

      case enums::Aggregation::value_of<"LAST">():
        return helpers::Aggregations::last(_begin, _end);

      case enums::Aggregation::value_of<"EWMA_1S">():
        return helpers::Aggregations::ewma(t1s, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_1M">():
        return helpers::Aggregations::ewma(t1m, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_1H">():
        return helpers::Aggregations::ewma(t1h, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_1D">():
        return helpers::Aggregations::ewma(t1d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_7D">():
        return helpers::Aggregations::ewma(t7d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_30D">():
        return helpers::Aggregations::ewma(t30d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_90D">():
        return helpers::Aggregations::ewma(t90d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_365D">():
        return helpers::Aggregations::ewma(t365d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_1S">():
        return helpers::Aggregations::ewma_trend(t1s, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_1M">():
        return helpers::Aggregations::ewma_trend(t1m, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_1H">():
        return helpers::Aggregations::ewma_trend(t1h, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_1D">():
        return helpers::Aggregations::ewma_trend(t1d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_7D">():
        return helpers::Aggregations::ewma_trend(t7d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_30D">():
        return helpers::Aggregations::ewma_trend(t30d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_90D">():
        return helpers::Aggregations::ewma_trend(t90d, _begin, _end);

      case enums::Aggregation::value_of<"EWMA_TREND_365D">():
        return helpers::Aggregations::ewma_trend(t365d, _begin, _end);

      case enums::Aggregation::value_of<"TIME SINCE FIRST MAXIMUM">():
        return helpers::Aggregations::time_since_first_maximum(_begin, _end);

      case enums::Aggregation::value_of<"TIME SINCE FIRST MINIMUM">():
        return helpers::Aggregations::time_since_first_minimum(_begin, _end);

      case enums::Aggregation::value_of<"TIME SINCE LAST MAXIMUM">():
        return helpers::Aggregations::time_since_last_maximum(_begin, _end);

      case enums::Aggregation::value_of<"TIME SINCE LAST MINIMUM">():
        return helpers::Aggregations::time_since_last_minimum(_begin, _end);

      case enums::Aggregation::value_of<"TREND">():
        return helpers::Aggregations::trend(_begin, _end);

      default:
        assert_msg(false, "Unknown aggregation for time-based column: '" +
                              _aggregation.name() + "'");
        return 0.0;
    }
  }

  /// Aggregates the matches using the extract_value lambda function.
  template <class ExtractValueType>
  static Float aggregate_matches_categorical(
      const std::vector<containers::Match> &_matches,
      const ExtractValueType &_extract_value,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature) {
    const auto is_non_null = [](Int val) { return val >= 0; };

    if (_abstract_feature.conditions_.size() == 0) {
      auto range = _matches | std::views::transform(_extract_value) |
                   std::views::filter(is_non_null);

      return aggregate_categorical_range(range.begin(), range.end(),
                                         _abstract_feature.aggregation_);
    }

    auto range = _matches | std::views::filter(_condition_function) |
                 std::views::transform(_extract_value) |
                 std::views::filter(is_non_null);

    return aggregate_categorical_range(range.begin(), range.end(),
                                       _abstract_feature.aggregation_);
  }

  /// Aggregates the range from _begin to _end, applying the _aggregation.
  template <class IteratorType>
  static Float aggregate_numerical_range(
      const IteratorType _begin, const IteratorType _end,
      const enums::Aggregation _aggregation) {
    switch (_aggregation.value()) {
      case enums::Aggregation::value_of<"AVG">():
        return helpers::Aggregations::avg(_begin, _end);

      case enums::Aggregation::value_of<"AVG TIME BETWEEN">():
        return calc_avg_time_between(_begin, _end);

      case enums::Aggregation::value_of<"COUNT">():
        return helpers::Aggregations::count(_begin, _end);

      case enums::Aggregation::value_of<"COUNT DISTINCT">():
        return helpers::Aggregations::count_distinct(_begin, _end);

      case enums::Aggregation::value_of<"COUNT DISTINCT OVER COUNT">():
        return helpers::Aggregations::count_distinct_over_count(_begin, _end);

      case enums::Aggregation::value_of<"COUNT MINUS COUNT DISTINCT">():
        return helpers::Aggregations::count(_begin, _end) -
               helpers::Aggregations::count_distinct(_begin, _end);

      case enums::Aggregation::value_of<"KURTOSIS">():
        return helpers::Aggregations::kurtosis(_begin, _end);

      case enums::Aggregation::value_of<"MAX">():
        return helpers::Aggregations::maximum(_begin, _end);

      case enums::Aggregation::value_of<"MEDIAN">():
        return helpers::Aggregations::median(_begin, _end);

      case enums::Aggregation::value_of<"MIN">():
        return helpers::Aggregations::minimum(_begin, _end);

      case enums::Aggregation::value_of<"MODE">():
        return helpers::Aggregations::mode<Float>(_begin, _end);

      case enums::Aggregation::value_of<"NUM MAX">():
        return helpers::Aggregations::num_max(_begin, _end);

      case enums::Aggregation::value_of<"NUM MIN">():
        return helpers::Aggregations::num_min(_begin, _end);

      case enums::Aggregation::value_of<"Q1">():
        return helpers::Aggregations::quantile(0.01, _begin, _end);

      case enums::Aggregation::value_of<"Q5">():
        return helpers::Aggregations::quantile(0.05, _begin, _end);

      case enums::Aggregation::value_of<"Q10">():
        return helpers::Aggregations::quantile(0.1, _begin, _end);

      case enums::Aggregation::value_of<"Q25">():
        return helpers::Aggregations::quantile(0.25, _begin, _end);

      case enums::Aggregation::value_of<"Q75">():
        return helpers::Aggregations::quantile(0.75, _begin, _end);

      case enums::Aggregation::value_of<"Q90">():
        return helpers::Aggregations::quantile(0.90, _begin, _end);

      case enums::Aggregation::value_of<"Q95">():
        return helpers::Aggregations::quantile(0.95, _begin, _end);

      case enums::Aggregation::value_of<"Q99">():
        return helpers::Aggregations::quantile(0.99, _begin, _end);

      case enums::Aggregation::value_of<"SKEW">():
        return helpers::Aggregations::skew(_begin, _end);

      case enums::Aggregation::value_of<"STDDEV">():
        return helpers::Aggregations::stddev(_begin, _end);

      case enums::Aggregation::value_of<"SUM">():
        return helpers::Aggregations::sum(_begin, _end);

      case enums::Aggregation::value_of<"VAR">():
        return helpers::Aggregations::var(_begin, _end);

      case enums::Aggregation::value_of<"VARIATION COEFFICIENT">():
        return helpers::Aggregations::variation_coefficient(_begin, _end);

      default:
        assert_msg(false, "Unknown aggregation for time-based column: '" +
                              _aggregation.name() + "'");
        return 0.0;
    }
  }

  /// Creates a key-value-pair for applying the FIRST and LAST aggregation.
  template <class ExtractValueType>
  static Float apply_first_last(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const std::vector<containers::Match> &_matches,
      const ExtractValueType &_extract_value,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization) {
    assert_true(is_first_last(_abstract_feature.aggregation_));

    assert_true(_peripheral.num_time_stamps() > 0);

    using Pair = std::pair<Float, Float>;

    const auto &ts_col_input = _peripheral.time_stamp_col();

    const auto extract_pair =
        [_extract_value, &ts_col_input](const containers::Match &m) -> Pair {
      const auto key = ts_col_input[m.ix_input];
      const auto value = _extract_value(m);
      return std::make_pair(key, value);
    };

    memorize_pairs_range(_matches, extract_pair, _condition_function,
                         _abstract_feature, _memoization);

    if (_abstract_feature.aggregation_.value() ==
            enums::Aggregation::value_of<"FIRST">() ||
        _abstract_feature.aggregation_.value() ==
            enums::Aggregation::value_of<"LAST">()) {
      return aggregate_first_last(_memoization->pairs_begin(),
                                  _memoization->pairs_end(),
                                  _abstract_feature.aggregation_);
    }

    assert_true(_population.num_time_stamps() > 0);

    const auto &ts_col_output = _population.time_stamp_col();

    const auto ts_output =
        _matches.size() > 0 ? ts_col_output[_matches[0].ix_output] : NAN;

    const auto substract_from_ts_output = [ts_output](const Pair &_p) -> Pair {
      const auto diff = ts_output - std::get<0>(_p);
      return std::make_pair(diff, std::get<1>(_p));
    };

    const auto pairs =
        fct::Range(_memoization->pairs_begin(), _memoization->pairs_end());

    auto range = pairs | std::views::transform(substract_from_ts_output);

    return aggregate_first_last(range.begin(), range.end(),
                                _abstract_feature.aggregation_);
  }

  /// Calculates the average time between the time stamps.
  template <class IteratorType>
  static Float calc_avg_time_between(const IteratorType _begin,
                                     const IteratorType _end) {
    const auto count = helpers::Aggregations::count(_begin, _end);

    if (count <= 1.0) {
      return 0.0;
    }

    const auto max_value = aggregate_numerical_range(
        _begin, _end, enums::Aggregation::make<"MAX">());

    const auto min_value = aggregate_numerical_range(
        _begin, _end, enums::Aggregation::make<"MIN">());

    return (max_value - min_value) / (count - 1.0);
  }

  /// Determines whether a value is nan or inf
  static bool is_not_nan_or_inf(const Float _val) {
    return !std::isnan(_val) && !std::isinf(_val);
  }

  /// Determines whether a value is nan or inf
  static bool second_is_not_nan_or_inf(const std::pair<Float, Float> &_p) {
    return !std::isnan(_p.second) && !std::isinf(_p.second);
  }

  /// Memorizes the range for numerical values.
  template <class ExtractValueType>
  static void memorize_numerical_range(
      const std::vector<containers::Match> &_matches,
      const ExtractValueType &_extract_value,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization) {
    if (_abstract_feature.conditions_.size() == 0) {
      const auto range = _matches | std::views::transform(_extract_value) |
                         std::views::filter(is_not_nan_or_inf);
      _memoization->memorize_numerical(_abstract_feature, range);
    }
    const auto range = _matches | std::views::filter(_condition_function) |
                       std::views::transform(_extract_value) |
                       std::views::filter(is_not_nan_or_inf);
    _memoization->memorize_numerical(_abstract_feature, range);
  }

  /// Memorizes the range for pairs.
  template <class ExtractValueType>
  static void memorize_pairs_range(
      const std::vector<containers::Match> &_matches,
      const ExtractValueType &_extract_value,
      const std::function<bool(const containers::Match &)> &_condition_function,
      const containers::AbstractFeature &_abstract_feature,
      const rfl::Ref<Memoization> &_memoization) {
    assert_true(is_first_last(_abstract_feature.aggregation_));
    if (_abstract_feature.conditions_.size() == 0) {
      const auto range = _matches | std::views::transform(_extract_value) |
                         std::views::filter(second_is_not_nan_or_inf);
      _memoization->memorize_pairs(_abstract_feature, range);
    }
    const auto range = _matches | std::views::filter(_condition_function) |
                       std::views::transform(_extract_value) |
                       std::views::filter(second_is_not_nan_or_inf);
    _memoization->memorize_pairs(_abstract_feature, range);
  }
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

#endif  // FASTPROP_ALGORITHM_AGGREGATOR_HPP_
